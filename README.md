# dbt on Amazon EMR Serverless via Spark Connect + S3 Tables

A complete, Terraform-managed example that runs **dbt** transformations on
**Amazon EMR Serverless** (Apache Spark **4.0.2**, release `emr-spark-8.0.0`) using
**Apache Spark Connect**, writing to **Amazon S3 Tables** (managed Apache Iceberg).
Jobs run from your **laptop** and on a schedule via **Amazon MWAA** (managed
Airflow). The generated dbt documentation (model lineage + data catalog) is
published as a **password-protected** static site on **Amazon CloudFront**.

It uses the **stock, released `dbt-spark` adapter** (no fork): the `session`
connection method plus the `SPARK_REMOTE` environment variable make PySpark open a
remote Spark Connect session on EMR Serverless. This is the approach AWS documents
in [Using dbt-core with EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/tutorials-dbt.html).

> Inspired by the pattern in
> [Serverless analytics pipelines using the Apache Spark engine in Amazon Athena](https://aws.amazon.com/blogs/big-data/serverless-analytics-pipelines-using-the-apache-spark-engine-in-amazon-athena/),
> which injects the session endpoint via `SPARK_REMOTE` the same way.

---

## Architecture

```
                        ┌──────────────────────────────────────────────────────────┐
                        │                        Your VPC                           │
     laptop (uv)        │                                                            │
   ┌────────────────┐   │   private subnets                    public subnets       │
   │ run_dbt.py     │   │  ┌───────────────────────┐          ┌──────────────────┐  │
   │ dbt-spark      │   │  │ EMR Serverless workers│          │  NAT gateway      │  │
   │ [session]      │   │  │ (Spark 4.0.2 driver / │──────────▶  → internet       │  │
   └──────┬─────────┘   │  │  executors, ENIs)     │          └──────────────────┘  │
          │             │  └───────────┬───────────┘   ┌───────────────────────────┐│
          │ 1 StartSession/            │               │ VPC endpoint:             ││
          │   GetSessionEndpoint       │ S3 gateway    │ emr-serverless-services   ││
          │ (boto3, SigV4)             ▼ endpoint      │ .sessions (private DNS)   ││
          │              ┌───────────────┐             └────────────▲──────────────┘│
          │              │  S3 Tables    │    ┌───────────────────────────┐         │
          ▼              │  (Iceberg)    │    │  MWAA (Airflow) workers    │─────────┘
  export SPARK_REMOTE=   │  namespace:   │◀───│  isolated venv (PyPI)      │  in-VPC gRPC
  sc://<host>:443/;      │  analytics    │    │  same run_dbt.py           │  to session
  use_ssl=true;          └───────────────┘    └───────────────────────────┘
  x-aws-proxy-auth=<token>
  2 Spark Connect (gRPC/443) ───────────────▶ (laptop: public endpoint; MWAA: VPC endpoint)
```

1. The runner calls `StartSession` + `GetSessionEndpoint` (IAM/SigV4) to get an
   endpoint host and a short-lived (~1 hour) `x-aws-proxy-auth` token.
2. It exports `SPARK_REMOTE`; dbt-spark's `session` method calls
   `SparkSession.builder.getOrCreate()`, which reads `SPARK_REMOTE` and opens a
   remote Spark Connect session (`sc://host:443`, TLS). The remote driver
   reads/writes the S3 Tables Iceberg catalog using the EMR Serverless
   **execution role**.

### What gets created

| Component | Resource |
|---|---|
| Network | Best-practices VPC: public + private subnets, NAT, **S3 gateway endpoint**, interface endpoints (STS, Logs, EMR Serverless, and **`emr-serverless-services.sessions`** for in-VPC Spark Connect) |
| Data | **S3 Tables** table bucket + `analytics` namespace (Iceberg warehouse) |
| Compute | **EMR Serverless** Spark application (`emr-spark-8.0.0`) on private subnets, interactive **Spark Connect sessions enabled** |
| Orchestration | **MWAA** running two DAGs: `dbt_emr_serverless_spark_connect` (daily `dbt build`) and `dbt_docs_refresh` (daily docs generate + publish). Both install the stock adapter from PyPI at task time |
| Docs UI | Password-protected **dbt docs** (lineage + catalog) on **CloudFront** — private S3 origin via OAC + HTTP Basic Auth enforced by a CloudFront Function |
| IAM | EMR execution role (scoped to the table bucket), a reusable *session-caller* policy, MWAA role |

### The dbt models

Classic jaffle-shop-style project, all Iceberg on S3 Tables:

- **seeds** `raw_customers`, `raw_orders`, `raw_payments` → the raw tables created on S3 Tables.
- **staging** `stg_*` → *ephemeral* (inlined as CTEs; no view/table objects).
- **marts** `dim_customers` (Iceberg table) and `fct_orders` (**incremental**, Iceberg `MERGE` on `order_id`).

A small macro override (`dbt/macros/drop_relation.sql`) makes table drops use
`PURGE` so re-runs are idempotent on S3 Tables (see [How it works](#s3-tables-catalog--ctas-drops-and-idempotency)).

### Iceberg partitioning & per-column statistics

`fct_orders` shows how to reach Iceberg's table features straight from a dbt
`config()` block — no bespoke macros required:

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="order_id",
        file_format="iceberg",
        partition_by="days(order_date)",
        incremental_predicates=[
            "DBT_INTERNAL_DEST.order_date >= date_add(current_date(), -"
                ~ var("merge_lookback_days", 3) ~ ")"
        ],
        tblproperties={
            "write.metadata.metrics.default": "counts",
            "write.metadata.metrics.column.amount": "full",
            "write.metadata.metrics.column.status": "full"
        }
    )
}}
```

- **Partition transform.** dbt passes `partition_by` straight into
  `PARTITIONED BY (...)`, so Iceberg transforms work as-is:
  `days()` / `months()` / `years()` / `hours()`, `bucket(N, col)`, and
  `truncate(N, col)`. This is *hidden* partitioning — queries filtering on
  `order_date` get partition pruning without a separate partition column, and
  you can evolve the spec later without rewriting data. Pick a granularity that
  matches volume (`months`/`years` for sparse data) to avoid many tiny files.
- **Per-column statistics.** `tblproperties` is a free-form dict handed to
  `TBLPROPERTIES (...)`. The `write.metadata.metrics.*` keys control the
  lower/upper bounds and value/null counts Iceberg records per column in each
  manifest; the planner uses them for scan pruning and predicate pushdown.
  `default = counts` for every column, with `full` stats collected for
  `amount` and `status`.
- **MERGE partition pruning (`incremental_predicates`).** dbt-spark ANDs these
  into the merge `ON` clause next to the `unique_key` match, so incremental runs
  emit:
  ```sql
  merge into analytics.fct_orders as DBT_INTERNAL_DEST
      using fct_orders__dbt_tmp as DBT_INTERNAL_SOURCE
      on DBT_INTERNAL_DEST.order_date >= date_add(current_date(), -3)
             and DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
      when matched then update set *
      when not matched then insert *
  ```
  The bound is on the partition column, so Iceberg prunes to the recent
  day-partitions instead of scanning the whole target to find `order_id`
  matches — the key performance lever on large Iceberg tables. Tune the window
  with `--vars '{merge_lookback_days: 30}'`.
  > **Correctness tradeoff:** a predicate in `ON` also gates matching. A source
  > row whose key exists in the target but in a partition *outside* the window
  > won't match and gets inserted as a duplicate, so the window must cover every
  > partition where an update can land. Safe here because the model only
  > processes orders newer than the current max `order_date`, and the `unique`
  > test on `order_id` is a backstop.

Verify it on the live table (Spark Connect session):

```sql
DESCRIBE TABLE EXTENDED s3tablesbucket.analytics.fct_orders;   -- # Partitioning -> Part 0: days(order_date)
SHOW TBLPROPERTIES  s3tablesbucket.analytics.fct_orders;       -- write.metadata.metrics.* = counts/full
SELECT * FROM       s3tablesbucket.analytics.fct_orders.partitions;  -- rows grouped by order_date_day
```

> **Gotcha — comments and Jinja in `config()`.** Keep the `config()` block clean
> (see `dbt/models/marts/fct_orders.sql`). Two traps bite here: (1) a SQL `--`
> comment placed *inside* a `{{ ... }}` expression is parsed as arithmetic, not a
> comment, so it breaks the `config()` call; put explanatory comments on their own
> lines *above* the block. (2) dbt renders Jinja everywhere in the file, comments
> included, so writing a literal `{{ config() }}` in a normal comment executes it
> as an empty config call and fails parsing. Never embed `{{ ... }}` braces in
> comments.

---

## Prerequisites

- **Terraform** >= 1.5 and the **AWS CLI v2**, both authenticated to your account.
  The AWS CLI is required *during* `terraform apply` (see [How it works](#enabling-sessions-sessionenabled)).
- **[uv](https://docs.astral.sh/uv/)** for local dbt runs.
- An AWS account in a region offering EMR Serverless `emr-spark-8.0.0`, S3 Tables and MWAA (e.g. `us-east-1`, `us-west-2`, `eu-west-1`).
- Permissions to create VPC, EMR Serverless, S3 Tables, MWAA, IAM resources.

---

## Deploy

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # edit region, name_prefix, etc.
terraform init
terraform apply
```

`apply` takes ~25-40 minutes (MWAA is the long pole). It will:

1. Build the VPC (incl. the Spark Connect sessions VPC endpoint), S3 Tables bucket, EMR Serverless app and IAM roles.
2. Enable Spark Connect sessions on the app (`sessionEnabled=true`) via the AWS CLI.
3. Upload the DAG, dbt project and runner to the MWAA bucket, then create MWAA.
4. Write `generated/emr_config.json` — the single source of truth the runner reads.

For a laptop-only deploy (no scheduler), set `enable_mwaa = false`.

---

## Run dbt from your laptop

Attach the generated *session-caller* policy to the IAM identity you use locally:

```bash
cd terraform
aws iam attach-user-policy \
  --user-name "$(aws sts get-caller-identity --query 'Arn' --output text | awk -F/ '{print $NF}')" \
  --policy-arn "$(terraform output -raw session_caller_policy_arn)"
# (or attach it to your role)
```

Then run dbt through the runner (uv installs the stock adapter + PySpark 4.0.2 to match EMR):

```bash
cd ../runner
uv sync
uv run python run_dbt.py -- build         # seed + run + test in one session
uv run python run_dbt.py -- run --select fct_orders
uv run python run_dbt.py --no-terminate -- run   # keep the session for debugging
```

The runner starts a Spark Connect session, exports `SPARK_REMOTE` and
`DBT_SPARK_SCHEMA`, runs dbt, then terminates the session.

Inspect the resulting tables:

```bash
aws s3tables list-tables \
  --table-bucket-arn "$(terraform -chdir=../terraform output -raw table_bucket_arn)" \
  --namespace analytics
```

---

## Run on a schedule (MWAA)

Open the Airflow UI and enable / trigger the DAG:

```bash
terraform -chdir=terraform output -raw mwaa_webserver_url
```

The `dbt_emr_serverless_spark_connect` DAG runs daily. Its `dbt_build` task shells
out to `mwaa_run_dbt.sh`, which installs the **stock** `dbt-spark[session]` +
`pyspark[connect]` from PyPI into an **isolated venv** on the worker (keeping dbt's
heavy dependency tree away from Airflow's own environment) and runs the **same**
`run_dbt.py`. Because MWAA's `dags/` folder is read-only, the bootstrap runs dbt
from a writable copy of the project under `/tmp`.

A second DAG, **`dbt_docs_refresh`**, regenerates and republishes the docs site on
the same schedule (see [Browse the dbt docs UI](#browse-the-dbt-docs-ui-lineage--catalog)).
Both DAGs are independent — trigger either from the Airflow UI whenever you want.

---

## Browse the dbt docs UI (lineage + catalog)

dbt renders your project as a browsable site with a model **lineage graph** and a
**data catalog** (columns, types, descriptions, stats). This example publishes it
behind a password on CloudFront and can refresh it on a schedule.

Set `enable_docs_site = true` (see [Key configuration](#key-configuration)) and
`terraform apply`. The first apply seeds the site with a small **placeholder**
page — a real docs page can't exist until the tables do, and seeding a placeholder
lets the CloudFront distribution come up cleanly instead of failing on a missing
file. From then on the content is refreshed **out-of-band**: the
`aws_s3_object.docs_index` resource carries `lifecycle { ignore_changes = [etag,
source] }`, so later `terraform apply` runs never revert a freshly generated site
back to the placeholder.

There are two ways to refresh the docs. Both run the **same** `run_dbt.py` and rely
on `dbt docs generate --static`, which introspects the warehouse and inlines the
lineage + catalog into one self-contained `target/static_index.html`. `run_dbt.py
--publish-docs` then uploads that file to `s3://<docs_bucket>/index.html` (the
bucket name comes from `emr_config.json`).

**A. On a schedule or on demand — the `dbt_docs_refresh` DAG.** A second MWAA DAG
runs the docs generation in a Spark Connect session and publishes the result. It's
scheduled `@daily` and can be triggered manually anytime (e.g. right after a model
change) from the Airflow UI. It's paused on first deploy — unpause it to start the
daily schedule.

**B. From your laptop — `--publish-docs`.** Generate and publish in one step
(handy for the first real page, or an ad-hoc refresh):

```bash
cd runner
uv run python run_dbt.py --publish-docs --minimal -- docs generate --static
# generates dbt/target/static_index.html and uploads it to s3://<docs_bucket>/index.html
```

Both refresh paths pass **`--minimal`**, which shrinks the docs session's Spark
workers to a small **2 vCPU** driver and executors (vs the 4 vCPU default). `dbt docs
generate` only introspects the catalog, so it doesn't need the compute a `dbt build`
does — a smaller footprint keeps the session well under the application's
`maximumCapacity`, so a docs refresh is far less likely to fail with
`ServiceQuotaExceededException` when a build session runs at the same time.

> Note on the knob: the obvious lever — capping the executor *count* via
> `spark.dynamicAllocation.maxExecutors` — makes EMR Serverless Spark Connect
> sessions fail to schedule (`Session scheduling timed out`). Shrinking the worker
> *size* is the reliable way to make a session smaller; EMR rounds the requested
> memory (heap + ~10% overhead) up to the nearest supported worker size.

The S3 write permission is granted (only when `enable_docs_site = true`) to both the
MWAA execution role and the *session-caller* policy, so the DAG and local runs can
publish.

> `dbt docs generate` introspects the warehouse, so refresh the docs **after** the
> tables exist (i.e. after at least one `dbt build`).

**Open it.** Grab the URL and credentials from the outputs:

```bash
terraform output docs_url
terraform output docs_username
terraform output -raw docs_password    # password is a sensitive output
```

Visit the URL; the browser prompts for the username/password. Leave
`docs_basic_auth_password` empty to auto-generate a strong password (surfaced by
the output above), or set your own.

---

## How it works

### Stock dbt-spark + `SPARK_REMOTE`

EMR Serverless exposes interactive sessions over Spark Connect
([docs](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/spark-connect.html)).
`GetSessionEndpoint` returns an endpoint host and a ~1-hour `x-aws-proxy-auth`
token. The runner assembles them into a `SPARK_REMOTE` URL:

```
sc://<host>:443/;use_ssl=true;x-aws-proxy-auth=<token>
```

PySpark's `SparkSession.builder.getOrCreate()` honors `SPARK_REMOTE` and opens a
*remote* Spark Connect session — so the **stock** `dbt-spark` `session` method
talks to EMR Serverless with no forked adapter. The dbt profile is minimal:

```yaml
type: spark
method: session
host: NA          # required by the schema but ignored when SPARK_REMOTE is set
schema: "{{ env_var('DBT_SPARK_SCHEMA', 'analytics') }}"
```

The client's PySpark version must match the EMR Spark version (`4.0.2` for
`emr-spark-8.0.0`), so both the runner and MWAA pin `pyspark[connect]==4.0.2`.

### Enabling sessions (`sessionEnabled`)

Spark Connect requires the application's `interactiveConfiguration.sessionEnabled = true`.
The Terraform `aws`/`awscc` providers don't expose that field yet (only `livy`/`studio`),
so Terraform creates the app and a `null_resource` sets (and verifies) the flag with the
AWS CLI (`update-application`). **This is why the AWS CLI must be available at apply time.**

Because the provider doesn't manage the flag, it *clears* `interactiveConfiguration`
whenever it updates the application for any other reason — which would silently disable
Spark Connect. So the `null_resource` re-asserts the flag on **every** apply: it's a fast
no-op when sessions are already on, and otherwise stops the app (UpdateApplication needs a
STOPPED app), sets the flag, and verifies it with retries. If you ever hit
`ValidationException: Sessions are not enabled on this application`, re-run `terraform apply`
(or `terraform apply -replace=null_resource.enable_spark_connect_sessions`).

### S3 Tables catalog — CTAS, drops, and idempotency

dbt's `table` materialization uses `CREATE TABLE AS SELECT`. The **S3 Tables Iceberg
REST endpoint does not support CTAS**, so this example uses the **S3 Tables Catalog
for Apache Iceberg** client (`S3TablesCatalog`), which does. The catalog config is
injected into every session:

```
spark.sql.catalog.s3tablesbucket             = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.s3tablesbucket.catalog-impl = software.amazon.s3tables.iceberg.S3TablesCatalog
spark.sql.catalog.s3tablesbucket.warehouse   = arn:aws:s3tables:<region>:<acct>:bucket/<name>
spark.sql.defaultCatalog                     = s3tablesbucket
spark.sql.extensions                         = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

**Idempotency gotcha:** S3 Tables rejects `DROP TABLE` without purge
(`dropTable(purge=false)` → error). dbt drops+recreates on seed full-refresh and
table replacement, so a *second* run fails unless the drop uses `PURGE`. The
project overrides `spark__drop_relation` (`dbt/macros/drop_relation.sql`) to emit
`DROP TABLE ... PURGE`, which Spark 4.0.2 honors — making re-runs idempotent.

`emr-spark-8.0.0` advertises native S3 Tables integration, so `s3tables_spark_packages`
defaults to empty (rely on the bundled classes). If your release doesn't bundle them,
set it to `software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8`
(matching your Spark/Iceberg versions).

### Reaching the session endpoint from MWAA (VPC endpoint)

From a laptop on the public internet, the session endpoint
(`*.s.emr-serverless-services.<region>.amazonaws.com`) is reachable directly. But an
MWAA worker in a **private subnet** routing gRPC out through the NAT gateway hits
`Stream removed (Data frame with END_STREAM flag received)` — the long-lived
HTTP/2 stream doesn't survive that path.

The fix is an interface VPC endpoint for
**`com.amazonaws.<region>.emr-serverless-services.sessions`** with private DNS. In-VPC
clients then resolve the session endpoint to private ENIs and reach it over the VPC
(no NAT), and the Spark Connect stream stays healthy. (Laptop clients keep using the
public endpoint.)

### Password-protecting the docs site

The docs HTML lives in a **private** S3 bucket (all public access blocked).
CloudFront reads it through an **Origin Access Control (OAC)**, so the bucket is
only reachable via the distribution. A **CloudFront Function** on the
`viewer-request` event enforces HTTP Basic Auth at every edge location:

```js
function handler(event) {
    var request = event.request;
    var expected = "Basic <base64(user:password)>"; // injected by Terraform
    if (!request.headers.authorization || request.headers.authorization.value !== expected) {
        return { statusCode: 401, statusDescription: "Unauthorized",
                 headers: { "www-authenticate": { value: "Basic realm=\"dbt docs\"" } } };
    }
    return request;
}
```

CloudFront Functions run at all edges (unlike Lambda@Edge, no `us-east-1`
requirement), and credentials travel only over HTTPS (CloudFront redirects HTTP →
HTTPS). Terraform precomputes `base64("user:password")` and embeds it in the
function. Prefer IP allow-listing instead? Swap the function check for a
`request.clientIp` test, or attach an AWS WAF IP set to the distribution.

> Note: because the credential is embedded in the (readable) function code, treat
> this as "keep casual visitors out", not as protection for sensitive data.

---

## Security

A static security review of the Terraform stack lives under
[`security/`](./security):

- [`summarize.md`](./security/summarize.md) — a triaged, opinionated read of the
  findings, grouped by what they mean for *this* architecture (not a raw tool
  dump). Start here.
- [`checkov.log`](./security/checkov.log) and [`trivy.log`](./security/trivy.log)
  — the raw Checkov and Trivy output. Reproduce with the commands in
  `summarize.md`. The downloaded community VPC module is excluded (we don't own
  that code).

The example favors convenience over hardening in a few deliberate spots that the
review calls out. The most important one for production:

> **Put the MWAA web server inside the VPC.** This example defaults to
> `mwaa_webserver_access_mode = "PUBLIC_ONLY"`, which exposes the Airflow UI on a
> public (IAM-authenticated) endpoint for easy laptop access. For production, set
> `mwaa_webserver_access_mode = "PRIVATE_ONLY"`: the web server is then reachable
> **only from inside the VPC** (via VPN, AWS Client VPN, a bastion, or
> PrivateLink), removing it from the public internet and shrinking the attack
> surface. The scheduler and workers already run in your private subnets
> regardless of this setting — it only controls exposure of the UI.

Other production hardening flagged in the review: narrow the security-group
egress from all-traffic to `tcp/443`, and drop the `force_destroy` flags on the
S3 buckets (and the S3 Tables bucket) so data can't be emptied on `destroy`.

---

## Cost & cleanup

- **MWAA** (`mw1.small`) bills continuously (~\$0.49/hr + storage) — the largest fixed cost.
- **EMR Serverless** bills per-second only while a session/job runs; the app auto-stops after `emr_idle_timeout_minutes`.
- **NAT gateway** and the **interface VPC endpoints** bill hourly (plus per-GB).
- **S3 Tables** bills for storage + requests.
- **CloudFront + the docs site** are negligible at this scale (PriceClass_100, a tiny static file, CloudFront Functions are fractions of a cent per million requests).

### Teardown / start from scratch

dbt creates the S3 Tables *tables* at runtime (seeds + models), so they are not
in Terraform state. The table bucket has `force_destroy = true`, but that does
**not** rescue the separate `aws_s3tables_namespace` resource, which Terraform
destroys *before* the bucket — and `DeleteNamespace` fails while the namespace
still holds tables. So empty the namespace first, then destroy:

```bash
cd terraform

# 1) Delete every table dbt created in the namespace (this is the "delete the
#    data" step — S3 Tables data is unrecoverable afterwards).
TB_ARN=$(terraform output -raw table_bucket_arn)
NS=$(terraform output -raw s3tables_namespace)
for t in $(aws s3tables list-tables --table-bucket-arn "$TB_ARN" --namespace "$NS" \
             --query 'tables[].name' --output text); do
  echo "deleting $NS.$t"
  aws s3tables delete-table --table-bucket-arn "$TB_ARN" --namespace "$NS" --name "$t"
done

# 2) Destroy all infrastructure. MWAA deletion is the long pole (~20-25 min).
terraform destroy
```

Notes:

- **EMR Serverless app** must be `STOPPED` to be deleted (it auto-stops after
  `emr_idle_timeout_minutes`; a run's session also self-terminates). If a session
  is live, wait for the idle timeout or terminate it first.
- **S3 buckets** (`artifacts`, `docs`) have `force_destroy = true`, so destroy
  empties them for you. Remove these flags (and the table bucket's) for prod.
- **IAM roles** — destroy calls `iam:DeleteRole` for the EMR execution and MWAA
  roles. Some restricted sessions (e.g. scoped-down SSO/Isengard roles) deny it;
  if destroy fails on the roles, re-run it with a principal that allows
  `DeleteRole`, or delete the two roles (`<name_prefix>-emr-exec`,
  `<name_prefix>-mwaa-exec`) in the IAM console and re-run `terraform destroy`.

**Start from scratch:** after a clean destroy, just redeploy —
`terraform apply` recreates everything and regenerates
`generated/emr_config.json` (and re-enables Spark Connect sessions), then re-run
`dbt build` (from your laptop or MWAA) to repopulate the tables.

---

## Key configuration

| Variable | Default | Notes |
|---|---|---|
| `region` | `us-east-1` | Must offer EMR Serverless `emr-spark-8.0.0`, S3 Tables, MWAA |
| `emr_release_label` | `emr-spark-8.0.0` | Apache Spark 4.0.2 |
| `emr_architecture` | `ARM64` | Graviton workers |
| `s3tables_spark_packages` | `""` | Set to the client-catalog runtime JAR if not bundled |
| `dbt_spark_pip_spec` | `dbt-spark[session]==1.11.0b2` | Stock adapter installed in MWAA; keep in sync with `runner/pyproject.toml` |
| `pyspark_version` | `4.0.2` | Must match the EMR Serverless Spark version |
| `enable_mwaa` | `true` | Set `false` for a laptop-only deploy |
| `mwaa_webserver_access_mode` | `PUBLIC_ONLY` | Set `PRIVATE_ONLY` to keep the Airflow UI inside the VPC (production hardening — see [Security](#security)) |
| `enable_interface_endpoints` | `true` | Includes the `emr-serverless-services.sessions` endpoint (needed for MWAA gRPC) |
| `enable_docs_site` | `false` | Publish the password-protected dbt docs UI on CloudFront |
| `docs_basic_auth_username` | `dbt` | Basic Auth username for the docs site |
| `docs_basic_auth_password` | `""` | Basic Auth password; empty auto-generates a strong one (sensitive output) |

---

## Repository layout

```
terraform/        VPC (+ Spark Connect sessions endpoint), S3 Tables, EMR Serverless, IAM, MWAA, packaging
  docs_site.tf    private S3 + CloudFront + Basic Auth function for the dbt docs UI
  templates/      basic_auth.js CloudFront Function
dbt/              dbt project (seeds, ephemeral staging, Iceberg marts)
  macros/         drop_relation.sql (PURGE override for S3 Tables idempotency)
runner/           uv-managed Python: Spark Connect session mgmt + run_dbt.py
airflow/dags/     MWAA DAGs: build (dbt_emr_serverless_dag.py) + docs refresh (dbt_docs_refresh_dag.py) + venv bootstrap (mwaa_run_dbt.sh)
generated/        emr_config.json + docs/index.html (written before/at apply)
security/         static security review: triaged summarize.md + raw checkov/trivy logs
```

---

## Caveats

- Auth tokens expire after ~1 hour; the runner opens a fresh session per invocation, so keep individual dbt runs under an hour (fine for this example).
- Lake Formation fine-grained access control is **not** supported for Spark Connect sessions, so this example uses IAM on the S3 Tables bucket.
- `dbt-spark 1.11.x` (which supports Spark 4 / dbt-core 1.12) is currently a pre-release on PyPI; pin an exact version for reproducibility (this repo pins `1.11.0b2`).
- dbt-spark's `session` method calls `enableHiveSupport()`, which Spark Connect politely rejects (`CANNOT_MODIFY_CONFIG`). It's a harmless warning (silenced via `PYTHONWARNINGS`), because the S3 Tables catalog is a named default catalog set on the remote session.
- Content from AWS documentation referenced here was paraphrased for licensing compliance.
```
