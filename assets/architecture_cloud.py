#!/usr/bin/env python3
"""Cloud-only architecture diagram for dbt on EMR Serverless (Spark Connect + S3 Tables).

Same engine as assets/architecture.py (the mingrammer `diagrams` library rendering
official AWS icons via Graphviz), but scoped to the AWS-hosted components only:
the local dbt runner / laptop is intentionally omitted. MWAA is the sole control
plane that opens the Spark Connect session and drives dbt.

Render it with:

    brew install graphviz            # one-time system dependency
    uv run --with diagrams python assets/architecture_cloud.py

Produces assets/architecture_cloud.png.
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import EMR
from diagrams.aws.management import Cloudwatch
from diagrams.aws.network import CloudFront, Endpoint, InternetGateway, NATGateway
from diagrams.aws.security import IAMRole
from diagrams.aws.storage import S3
from diagrams.onprem.client import Users
from diagrams.onprem.workflow import Airflow

graph_attr = {
    "fontsize": "16",
    "labelloc": "t",
    "pad": "0.6",
    "nodesep": "0.6",
    "ranksep": "1.0",
    "splines": "spline",
}

with Diagram(
    "dbt on EMR Serverless via Spark Connect  (AWS cloud, MWAA-driven)",
    filename="assets/architecture_cloud",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
):
    viewer = Users("Docs viewer\n(browser)")

    with Cluster("AWS Account - us-west-2"):
        artifacts = S3("Artifacts bucket\n(DAGs, runner, emr_config.json)")
        s3tables = S3("S3 Tables\n(Iceberg warehouse)")
        logs = Cloudwatch("CloudWatch Logs")

        with Cluster("Amazon MWAA"):
            mwaa = Airflow("MWAA - Airflow\nbuild + docs_refresh DAGs")

        with Cluster("VPC  10.20.0.0/16"):
            with Cluster("Public subnets"):
                nat = NATGateway("NAT GW")
                igw = InternetGateway("IGW")
            with Cluster("Private subnets"):
                emr = EMR("EMR Serverless\nSpark Connect session")
            with Cluster("VPC endpoints"):
                s3gw = Endpoint("S3 gateway\nendpoint")
                ifep = Endpoint("Interface endpoints\nemr-serverless / sessions\nsts / logs")

        with Cluster("dbt docs site"):
            docs_s3 = S3("Docs bucket\n(private, OAC)")
            cf = CloudFront("CloudFront\n+ Basic Auth function")

        with Cluster("IAM"):
            emr_role = IAMRole("EMR exec role")
            mwaa_role = IAMRole("MWAA exec role\n+ session-caller")

    # Control plane: MWAA opens the Spark Connect session and drives dbt
    mwaa >> Edge(label="StartSession + run dbt") >> ifep
    ifep >> emr
    artifacts >> Edge(label="sync DAGs / runner / config") >> mwaa

    # Data plane
    emr >> Edge(label="Iceberg read/write") >> s3gw >> s3tables
    emr >> Edge(label="driver / executor logs") >> logs
    emr >> Edge(label="PyPI egress (adapter install)") >> nat >> igw

    # Docs: publish out-of-band, serve via CloudFront
    mwaa >> Edge(label="docs_refresh", style="dashed", color="darkgreen") >> docs_s3
    viewer >> Edge(label="HTTPS + Basic Auth") >> cf >> Edge(label="OAC origin") >> docs_s3

    # IAM associations
    emr_role >> Edge(style="dotted", color="gray") >> emr
    mwaa_role >> Edge(style="dotted", color="gray") >> mwaa
