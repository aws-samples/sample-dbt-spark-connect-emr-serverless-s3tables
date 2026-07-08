"""Manage Amazon EMR Serverless interactive Spark Connect sessions.

The flow implemented here mirrors the EMR Serverless Spark Connect user guide:

  1. Ensure the application is STARTED (auto-start also handles this).
  2. StartSession with the execution role + Spark runtimeConfiguration.
  3. Poll GetSession until the session reaches STARTED / IDLE.
  4. GetSessionEndpoint to obtain the endpoint hostname and a ~1 hour auth token.
  5. Hand the host + token to the caller (dbt uses them via Spark Connect).
  6. TerminateSession when finished (billing stops).

The endpoint host + token become the SPARK_REMOTE URL that pyspark consumes:

    sc://<host>:443/;use_ssl=true;x-aws-proxy-auth=<token>
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3

logger = logging.getLogger("emr_serverless_session")

# Session states that mean the driver is ready to accept Spark Connect traffic.
READY_STATES = {"STARTED", "IDLE"}
TERMINAL_STATES = {"TERMINATED", "FAILED"}
APP_READY_STATES = {"STARTED"}
APP_STARTABLE_STATES = {"CREATED", "STOPPED"}


@dataclass
class SessionEndpoint:
    """Resolved Spark Connect endpoint for a session."""

    session_id: str
    host: str
    token: str
    expires_at: Optional[str] = None

    @property
    def spark_remote_url(self) -> str:
        """Value for the SPARK_REMOTE env var consumed by pyspark[connect].

        PySpark's SparkSession.builder.getOrCreate() reads SPARK_REMOTE and opens a
        remote Spark Connect session, so the stock dbt-spark `session` method talks
        to EMR Serverless with no forked adapter.
        """
        return f"sc://{self.host}:443/;use_ssl=true;x-aws-proxy-auth={self.token}"


class EmrServerlessSparkConnect:
    """Thin, dependency-light wrapper around the EMR Serverless session APIs."""

    def __init__(self, application_id: str, region: str, client: Any = None) -> None:
        self.application_id = application_id
        self.region = region
        self.client = client or boto3.client("emr-serverless", region_name=region)
        self.session_id: Optional[str] = None

    # ------------------------------------------------------------------ app
    def ensure_application_started(self, timeout: int = 300, poll: int = 10) -> None:
        """Start the application if it is not already running, then wait."""
        app = self.client.get_application(applicationId=self.application_id)["application"]
        state = app["state"]
        logger.info("Application %s is in state %s", self.application_id, state)

        if state in APP_READY_STATES:
            return
        if state in APP_STARTABLE_STATES:
            logger.info("Starting application %s ...", self.application_id)
            self.client.start_application(applicationId=self.application_id)

        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self.client.get_application(applicationId=self.application_id)["application"]["state"]
            if state in APP_READY_STATES:
                logger.info("Application is STARTED.")
                return
            logger.info("Waiting for application to start (state=%s)...", state)
            time.sleep(poll)
        raise TimeoutError(f"Application {self.application_id} did not start within {timeout}s")

    # -------------------------------------------------------------- session
    def start_session(
        self,
        execution_role_arn: str,
        runtime_configuration: Optional[List[Dict[str, Any]]] = None,
        idle_timeout_minutes: int = 30,
        name: Optional[str] = None,
    ) -> str:
        kwargs: Dict[str, Any] = {
            "applicationId": self.application_id,
            "executionRoleArn": execution_role_arn,
            "idleTimeoutMinutes": idle_timeout_minutes,
        }
        if runtime_configuration:
            kwargs["configurationOverrides"] = {"runtimeConfiguration": runtime_configuration}
        if name:
            kwargs["name"] = name

        resp = self.client.start_session(**kwargs)
        self.session_id = resp["sessionId"]
        logger.info("Started session %s", self.session_id)
        return self.session_id

    def wait_until_ready(self, timeout: int = 600, poll: int = 5) -> str:
        assert self.session_id, "start_session must be called first"
        deadline = time.time() + timeout
        while time.time() < deadline:
            session = self.client.get_session(
                applicationId=self.application_id, sessionId=self.session_id
            )["session"]
            state = session["state"]
            if state in READY_STATES:
                logger.info("Session %s is %s", self.session_id, state)
                return state
            if state in TERMINAL_STATES:
                details = session.get("stateDetails", "unknown")
                raise RuntimeError(f"Session {self.session_id} entered {state}: {details}")
            logger.info("Waiting for session (state=%s)...", state)
            time.sleep(poll)
        raise TimeoutError(f"Session {self.session_id} not ready within {timeout}s")

    def get_endpoint(self) -> SessionEndpoint:
        assert self.session_id, "start_session must be called first"
        resp = self.client.get_session_endpoint(
            applicationId=self.application_id, sessionId=self.session_id
        )
        # GetSessionEndpoint returns an https:// URL with no port; the client must
        # connect on 443. Strip the scheme to get the bare host for the sc:// URL.
        endpoint_url = resp["endpoint"]
        host = urlparse(endpoint_url).netloc or endpoint_url.replace("https://", "").replace("http://", "")
        host = host.split("/")[0]
        endpoint = SessionEndpoint(
            session_id=self.session_id,
            host=host,
            token=resp["authToken"],
            expires_at=resp.get("authTokenExpiresAt"),
        )
        logger.info("Resolved Spark Connect endpoint host=%s (token expires %s)", host, endpoint.expires_at)
        return endpoint

    def terminate(self) -> None:
        if not self.session_id:
            return
        try:
            self.client.terminate_session(
                applicationId=self.application_id, sessionId=self.session_id
            )
            logger.info("Terminated session %s", self.session_id)
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup
            logger.warning("Failed to terminate session %s: %s", self.session_id, exc)
