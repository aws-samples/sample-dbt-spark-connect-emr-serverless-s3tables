# emr-dbt-runner

Starts an Amazon EMR Serverless interactive **Spark Connect** session, exports the
endpoint as `SPARK_REMOTE` (`sc://host:443/;use_ssl=true;x-aws-proxy-auth=<token>`),
and runs dbt against the S3 Tables Iceberg catalog using the **stock**
`dbt-spark[session]` adapter (no fork). Managed with [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run python run_dbt.py -- build
```

Generate the dbt docs and publish them to the CloudFront docs site in one step
(`--publish-docs` uploads `target/static_index.html` to the docs bucket named in
`emr_config.json`):

```bash
uv run python run_dbt.py --publish-docs --minimal -- docs generate --static
```

`--minimal` caps the session to a small driver + one small executor (docs
generation is metadata-only). The same runner powers the MWAA DAGs
(`dbt_emr_serverless_spark_connect` and `dbt_docs_refresh`). See the repository
root `README.md` for full instructions.
