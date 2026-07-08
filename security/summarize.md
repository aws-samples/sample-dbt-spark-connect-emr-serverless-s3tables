# Security review — dbt on EMR Serverless (Terraform)

Static security scan of the `terraform/` stack, with a triaged, opinionated read
of the findings (not a raw tool dump). The downloaded community VPC module
(`terraform/.terraform/…`) is excluded — we don't own that code.

| | |
|---|---|
| **Date** | 2026-07-07 |
| **Scope** | `terraform/*.tf` (own code only) |
| **Tools** | Checkov `3.3.6`, Trivy `0.72.0` (Trivy bundles the former `tfsec` rules) |
| **Raw logs** | [`checkov.log`](./checkov.log), [`trivy.log`](./trivy.log) |

Reproduce:

```bash
# Checkov (broad AWS ruleset)
checkov -d terraform --skip-path '\.terraform' --compact

# Trivy (fast, complementary; excludes downloaded modules)
trivy config terraform --tf-exclude-downloaded-modules
```

## Results at a glance

| Tool | Result |
|---|---|
| Checkov | **139 passed / 27 failed** / 0 skipped |
| Trivy | **6 findings** in own code — 3 CRITICAL, 1 HIGH, 1 MEDIUM, 1 LOW |

The two tools overlap heavily on the security-group egress finding; the rest are
largely distinct. Below, findings are grouped by **what they actually mean for
this architecture**, not by tool or by the tool's raw severity label.

---

## 1. Worth fixing — genuine hardening

### 1.1 Security groups allow unrestricted egress (`0.0.0.0/0`, all ports)
- **IDs:** Trivy `AWS-0104` (CRITICAL ×3), Checkov `CKV_AWS_382`
- **Resources:** `aws_security_group.emr` (emr_serverless.tf:15), `aws_security_group.mwaa[0]` (mwaa.tf:135), `aws_security_group.vpce[0]` (vpc.tf:61)
- **Read:** All-egress is common and *partly* required — MWAA and EMR need outbound
  to AWS APIs and to PyPI (over the NAT gateway) to install the adapter at task
  time. But `protocol = "-1"` to `0.0.0.0/0` is broader than needed and is the main
  data-exfiltration path a reviewer will flag.
- **Recommended:** Narrow egress to `tcp/443` instead of all-ports/all-protocols.
  The **VPC-endpoint SG** (`vpce`) is the easy win — it only needs `443` from the
  VPC CIDR and has no reason to reach the internet at all. For the EMR/MWAA SGs,
  `tcp/443` egress keeps HTTPS to AWS APIs and PyPI working while dropping every
  other port/protocol.

### 1.2 MWAA webserver is publicly accessible
- **ID:** Checkov `CKV2_AWS_66`
- **Resource:** `aws_mwaa_environment.this[0]` (no `webserver_access_mode` set → defaults to `PUBLIC_ONLY`)
- **Read:** The Airflow UI is reachable from the internet (IAM-authenticated). That
  is a deliberate convenience for a laptop-driven demo, but for a best-practices
  posture the hardened setting is `PRIVATE_ONLY` (UI reachable only inside the VPC,
  via VPN/PrivateLink/bastion).
- **Decision required** (see §4).

### 1.3 CloudFront docs distribution hardening
- **IDs:** `CKV_AWS_86` (access logging), `CKV_AWS_68` + `CKV2_AWS_47` (WAF / Log4j AMR), `CKV2_AWS_32` (response-headers policy), `CKV_AWS_374` (geo restriction)
- **Resource:** `aws_cloudfront_distribution.docs[0]`
- **Read:** This is a public, password-protected static docs site. Cheap, high-value
  additions: **access logging** and a **security-headers response policy**
  (HSTS, `X-Content-Type-Options`, etc.). **WAF** is worthwhile but adds cost;
  reasonable to skip for a demo. Geo restriction is optional.

### 1.4 CloudFront TLS floor (default certificate)
- **IDs:** `CKV_AWS_174` (viewer cert TLS ≥ 1.2), `CKV2_AWS_42` (custom SSL cert)
- **Resource:** `aws_cloudfront_distribution.docs[0]`
- **Read:** Uses the default `*.cloudfront.net` certificate, which negotiates down
  to TLSv1. Enforcing `minimum_protocol_version = TLSv1.2_2021` requires a **custom
  domain + ACM certificate**. This is a structural limitation of the no-custom-domain
  demo, not a config oversight.
- **Decision required** (see §4).

### 1.5 S3 server access logging
- **IDs:** Checkov `CKV_AWS_18`, Trivy `AWS-0089` (LOW)
- **Resources:** `aws_s3_bucket.artifacts`, `aws_s3_bucket.docs[0]`
- **Read:** Neither bucket has access logging. Cheap auditability win; requires a
  dedicated log-destination bucket (which itself must use SSE-S3, not KMS).

### 1.6 S3 encryption keys
- **IDs:** Trivy `AWS-0132` (HIGH, artifacts), Checkov `CKV_AWS_145` (docs)
- **Resources:** `aws_s3_bucket.artifacts` (SSE-KMS with the AWS-managed key, not a CMK), `aws_s3_bucket.docs[0]` (no SSE block → default SSE-S3)
- **Read:** A customer-managed KMS key (CMK) adds key rotation, access policies and
  audit. Low priority for a demo, easy to add. Note: an access-log destination
  bucket must stay on SSE-S3.

### 1.7 docs bucket versioning
- **ID:** `CKV_AWS_21` — `aws_s3_bucket.docs[0]`
- **Read:** Low value here — the bucket holds a single static `index.html`
  republished by Terraform. Optional.

---

## 2. By-design / false positives — suppress with justification

These should **not** be "fixed"; suppress them (with a written reason) so the scan
goes green for the right reasons.

| Finding | Why it's not a real issue |
|---|---|
| `AWS-0342` — `iam:PassRole` (MEDIUM) | **By design and already least-privilege.** `PassRole` is scoped to the single `emr_exec` role ARN with condition `iam:PassedToService = emr-serverless.amazonaws.com`. |
| `CKV2_AWS_5` ×3 — "SG not attached to a resource" | **False positive.** The SGs are attached to the EMR app, the MWAA environment and the VPC endpoints; Checkov's graph doesn't trace those attributes. |
| `CKV2_AWS_6` — docs bucket public-access block | **False positive.** `aws_s3_bucket_public_access_block.docs` exists; a `count`-index quirk breaks the graph association. |
| `CKV_AWS_144` (cross-region replication), `CKV2_AWS_61` (lifecycle), `CKV2_AWS_62` (event notifications), `CKV_AWS_310` (CloudFront origin failover) | Availability / ops-cost features, **not security**, and unnecessary for demo artifact/docs buckets. |

`CKV_TF_1` (pin the community VPC module to a commit SHA) is minor supply-chain
hardening — optional, not a false positive.

---

## 3. What's already correct (139 Checkov passes)

Notable good defaults already in place:

- Compute (EMR Serverless, MWAA) runs in **private subnets**; no public ingress.
- **S3 public-access blocks** on the buckets; docs bucket is private behind
  CloudFront **OAC** (no public bucket policy).
- S3 Tables IAM is **scoped to the single table bucket** and its tables.
- EMR execution-role trust has a **confused-deputy guard** (`aws:SourceAccount`).
- **No hardcoded secrets**; the docs Basic Auth password is generated and marked a
  sensitive output.
- Artifacts bucket has versioning + SSE + public-access block.

---

## 4. Decisions needed before implementing

Two items change behaviour and need an explicit call rather than a silent "fix":

1. **MWAA `PUBLIC_ONLY` → `PRIVATE_ONLY`** (§1.2): hardened, but the Airflow UI then
   requires in-VPC access (VPN / PrivateLink / bastion). Keep public for demo
   convenience, or go private?
2. **CloudFront TLS floor** (§1.4): enforcing TLS 1.2+ requires a custom domain +
   ACM cert. Accept the default-cert limitation for the demo, or add a domain?

---

## 5. Suggested remediation order

1. Tighten SG egress to `tcp/443` (VPCE SG → VPC CIDR only). *(low risk)*
2. Add CloudFront access logging + a security-headers response policy. *(low risk)*
3. Add S3 server access logging (+ a log bucket on SSE-S3). *(low risk)*
4. Switch artifacts/docs buckets to a CMK. *(low risk)*
5. Suppress the by-design/false-positive findings in §2 with justifications.
6. Resolve the two §4 decisions.
7. Wire Checkov + Trivy into `pre-commit` / CI so this stays green over time.
