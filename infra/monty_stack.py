"""Monty CDK stack: 4 Docker-image Lambdas + API Gateway + EventBridge +
Secrets Manager.

Pattern mirrors `analytics-ingest-iterate-repo/CloudformationStack/
ai_ingest_stack_iterate.py` — same `DockerImageFunction` shape, same
Secrets Manager grant pattern, same naming convention.
"""

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
)
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_integrations as apigw_int
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import RemovalPolicy
from constructs import Construct

# Shared ECR repository name for all four Lambdas. Created out-of-band by
# `make build` (ecr-repo-ensure) before the first deploy.
_ECR_REPO_NAME = "monty"


class MontyStack(Stack):
    """Single stack housing all Monty resources for one env."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        image_tag: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = env_name

        # Require an explicit imageTag context (immutable ECR digest) on every
        # deploy. Falling back to a moving tag like ":latest" silently makes
        # CFN no-op on Lambda code updates whenever the tag string is
        # unchanged across deploys, leaving the running function frozen on a
        # stale image. The Makefile's `cdk-deploy` target resolves the
        # just-pushed ECR digest from `.image-digest` and passes it here.
        image_tag = (
            self.node.try_get_context("imageTag") or image_tag
        )
        if not image_tag:
            raise ValueError(
                "imageTag context is required. Pass via "
                "`cdk deploy -c imageTag=<sha256:...>` "
                "(the Makefile's cdk-deploy target does this automatically)."
            )
        self.image_tag = image_tag

        # All four Lambdas share one ECR image; only the `cmd` differs per
        # handler. Resolve the repo by name once and reuse.
        ecr_repo = ecr.Repository.from_repository_name(
            self, "MontyEcrRepo", repository_name=_ECR_REPO_NAME,
        )

        # Secret holds Snowflake creds + HMAC secret + Slack webhook URLs.
        # The routing defaults (INCIDENTS, ALERTS, DEV) are templated here so
        # every env ships with the slots; values are populated out-of-band
        # (template applies only at secret creation). Any extra `SLACK_WEBHOOK_*`
        # key the observer finds becomes an addressable channel — no code change.
        secret = secretsmanager.Secret(
            self,
            "MontySecret",
            secret_name=f"monty-{env_name}-secrets",
            description="Snowflake creds + HMAC + Slack webhooks for Monty",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"user":"","password":"","account":"",'
                '"warehouse":"MONTY_WH","database":"MONITORING_DB",'
                '"schema":"MONITORING","role":"MONTY_SVC_ROLE",'
                '"MONTY_HMAC_SECRET":"",'
                '"SLACK_WEBHOOK_INCIDENTS":"","SLACK_WEBHOOK_ALERTS":"",'
                '"SLACK_WEBHOOK_DEV":""}',
                generate_string_key="placeholder",
                exclude_punctuation=True,
            ),
        )

        # One IAM role shared by all four Lambdas. Permissions are narrow:
        # read this secret, write CloudWatch Logs. Snowflake access is via
        # the secret's password — no IAM-to-Snowflake federation in v1.
        role = iam.Role(
            self,
            "MontyLambdaRole",
            role_name=f"monty-{env_name}-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        secret.grant_read(role)

        # LEGACY (cutover): the former warning/info store — Parquet objects under
        # run_date=YYYYMMDD/. The live write path moved to the DynamoDB metrics
        # table below; the bucket stays (RETAIN + write grant + env var) so
        # pre-cutover history remains readable by the dashboard and an image
        # rollback to a pre-DynamoDB digest still writes cleanly. Decommission
        # only after the DynamoDB path is verified end-to-end.
        metrics_bucket = s3.Bucket(
            self,
            "MetricsBucket",
            bucket_name=f"monty-{env_name}-metrics",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        # The shared Lambda role only needs to put objects. grant_write covers
        # PutObject (+ multipart) without granting read/delete.
        metrics_bucket.grant_write(role)

        # DynamoDB table for low-priority (warning/info) metrics written by
        # lambdas/shared/dynamo_writer.py, instead of INSERTing them into
        # Snowflake (replaces the S3 Parquet store above). On-demand billing —
        # no idle capacity to manage; RETAIN so a stack teardown never drops
        # metric history; `ttl` attribute gives free retention-based expiry.
        # Key design: pk = "<env>#<pipeline_name>" (per-pipeline grouping),
        # sk = "<occurred_at ISO UTC>#<uuid4>" (time-sortable + unique), so
        # "recent N for pipeline X" is one Query with ScanIndexForward=False.
        # Named *-metrics-ddb to coexist with the retained bucket during cutover.
        metrics_table = dynamodb.Table(
            self,
            "MetricsTable",
            table_name=f"monty-{env_name}-metrics-ddb",
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="sk", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.RETAIN,
        )
        # grant_write_data covers PutItem/BatchWrite without read/delete.
        metrics_table.grant_write_data(role)

        # Cross-account read access for the tools account. A role in the
        # tools account assumes this role to Query the metrics table (Query
        # only — no Scan/GetItem/write, and no access to the prompt-logs
        # table, which holds raw prompt/response bodies).
        cross_account_query_role = iam.Role(
            self,
            "CrossAccountDynamoQueryRole",
            role_name="CrossAccountDynamoQueryRole",
            assumed_by=iam.AccountPrincipal("290606987225"),
            description="Role assumed by tools account for DynamoDB Query access",
        )
        cross_account_query_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    metrics_table.table_arn,
                    # Covers any GSI/LSI added later; the table has none today.
                    f"{metrics_table.table_arn}/index/*",
                ],
            )
        )

        # DynamoDB table logging every SweatAI prompt/response. On-demand
        # billing (PAY_PER_REQUEST) so an idle endpoint costs nothing and a
        # spike needs no capacity planning; RETAIN so a stack teardown never
        # drops the prompt history. Partition key `id` (a per-request UUID) +
        # sort key `created_at`, matching the columns the sweatai handler
        # writes (model, timestamp, system_prompt, user_prompt, response,
        # prompt_tokens, system_prompt_tokens, completion_tokens, duration_sec,
        # service). The Lambda writes here (grant_write_data below); the table
        # name reaches it via common_env.
        prompt_logs_table = dynamodb.Table(
            self,
            "SweatAiPromptLogs",
            table_name=f"monty-{env_name}-sweatai-prompt-logs",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        # grant_write_data covers PutItem/UpdateItem/BatchWrite without read.
        prompt_logs_table.grant_write_data(role)

        common_env = {
            "MONTY_SECRET_NAME": secret.secret_name,
            "MONTY_ENV": env_name,
            # Legacy env var kept through cutover so an image rollback to a
            # pre-DynamoDB digest (which writes S3) still finds its bucket.
            "MONTY_METRICS_BUCKET": metrics_bucket.bucket_name,
            "MONTY_METRICS_TABLE": metrics_table.table_name,
            "MONTY_PROMPT_LOGS_TABLE": prompt_logs_table.table_name,
            "PYTHONUNBUFFERED": "1",
        }

        # ---- 4 Docker image Lambdas ------------------------------------

        # CMD overrides per-handler. Dockerfile sets the python path to the
        # repo root so `lambdas.observer.handler.lambda_handler` resolves.
        observer_fn = self._docker_function(
            "Observer",
            cmd=["lambdas.observer.handler.lambda_handler"],
            common_env=common_env,
            role=role,
            timeout_seconds=120,
            ecr_repo=ecr_repo,
        )
        failure_proxy_fn = self._docker_function(
            "FailureProxy",
            cmd=["lambdas.failure_proxy.handler.lambda_handler"],
            common_env=common_env,
            role=role,
            timeout_seconds=30,
            ecr_repo=ecr_repo,
        )
        sns_subscriber_fn = self._docker_function(
            "SnsSubscriber",
            cmd=["lambdas.sns_subscriber.handler.lambda_handler"],
            common_env=common_env,
            role=role,
            timeout_seconds=60,
            ecr_repo=ecr_repo,
        )
        log_scanner_fn = self._docker_function(
            "LogScanner",
            cmd=["lambdas.log_scanner.handler.lambda_handler"],
            common_env=common_env,
            role=role,
            timeout_seconds=60,
            ecr_repo=ecr_repo,
        )
        # SweatAI: synchronous HTTP endpoint (POST /sweatai). Shares the same
        # image + role; handler validates the HMAC signature like failure_proxy
        # and returns its JSON response to the caller. Timeout sized for an
        # interactive request (may call out to an LLM / Snowflake).
        sweatai_fn = self._docker_function(
            "SweatAI",
            cmd=["lambdas.sweatai.handler.lambda_handler"],
            common_env=common_env,
            role=role,
            timeout_seconds=30,
            ecr_repo=ecr_repo,
        )

        # ---- API Gateway HTTP API in front of failure_proxy -------------
        http_api = apigw.HttpApi(
            self,
            "MontyHttpApi",
            api_name=f"monty-{env_name}-api",
            description="Monty failure-proxy ingress",
        )
        http_api.add_routes(
            path="/failure",
            methods=[apigw.HttpMethod.POST],
            integration=apigw_int.HttpLambdaIntegration(
                "FailureProxyIntegration", failure_proxy_fn
            ),
        )
        http_api.add_routes(
            path="/sweatai",
            methods=[apigw.HttpMethod.POST],
            integration=apigw_int.HttpLambdaIntegration(
                "SweatAiIntegration", sweatai_fn
            ),
        )

        # The observer routes its "Error by ai" snippet THROUGH the SweatAI
        # endpoint (lambdas/shared/sweatai_client) so those calls get logged to
        # the prompt-logs table. It needs the endpoint URL; the HMAC secret it
        # already reads from the Monty secret. Set here because the URL isn't
        # known until the HTTP API exists.
        observer_fn.add_environment(
            "SWEATAI_URL", http_api.api_endpoint + "/sweatai"
        )

        # ---- EventBridge: Observer, single lane -------------------------
        # History: a 1-minute poll kept the Snowflake warehouse permanently awake,
        # so we split into a fast lane (5 min, critical/error) + a batch lane
        # (warning/info) to cut idle load. Once warning/info moved to S3 Parquet
        # (metric_writer routing), CUSTOM_METRICS only holds critical/error, so
        # the batch lane swept nothing — its whole reason to exist was gone.
        # Collapsed back to ONE lane at 5 min: it reads every alertable row
        # (no severity filter) and covers all incidents the observer can deliver.
        # No `lane` field on the event → the handler applies no severity clause
        # (its "all" fallback), which is exactly what we want now.
        observer_schedule = events.Rule(
            self,
            "ObserverSchedule",
            rule_name=f"monty-{env_name}-observer-schedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            description="Observer: poll CUSTOM_METRICS for unsent alerts every 5 minutes",
        )
        observer_schedule.add_target(targets.LambdaFunction(observer_fn))

        # ---- Resource policies for cross-stack subscription ------------
        # Other AWS stacks (analytics-ingest-iterate-repo et al.) need
        # permission to invoke our log_scanner from their CloudWatch Logs
        # subscription filters and to publish their SNS topics into our
        # sns_subscriber. Granting the broad service principals here is
        # the simplest model; tighten with source ARNs once the upstream
        # repos are ready.
        log_scanner_fn.add_permission(
            "AllowCloudWatchLogsInvoke",
            principal=iam.ServicePrincipal(
                f"logs.{cdk.Aws.REGION}.amazonaws.com"
            ),
            action="lambda:InvokeFunction",
            source_account=cdk.Aws.ACCOUNT_ID,
        )
        sns_subscriber_fn.add_permission(
            "AllowSnsInvoke",
            principal=iam.ServicePrincipal("sns.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_account=cdk.Aws.ACCOUNT_ID,
        )

        # ---- Outputs (consumed by the dbt + AWS-ingest specs) ----------
        CfnOutput(
            self,
            "FailureProxyUrl",
            value=http_api.api_endpoint + "/failure",
            description="POST here with X-Monty-Signature to register a failure",
        )
        CfnOutput(
            self,
            "SweatAiUrl",
            value=http_api.api_endpoint + "/sweatai",
            description="POST here with X-Monty-Signature to reach the SweatAI endpoint",
        )
        CfnOutput(
            self,
            "SnsSubscriberArn",
            value=sns_subscriber_fn.function_arn,
            description="Subscribe ai-ingest-*-alerts SNS topics to this ARN",
        )
        CfnOutput(
            self,
            "LogScannerArn",
            value=log_scanner_fn.function_arn,
            description="Add CloudWatch Logs subscription filter to this ARN",
        )
        CfnOutput(self, "SecretName", value=secret.secret_name)
        CfnOutput(
            self,
            "MetricsBucketName",
            value=metrics_bucket.bucket_name,
            description="LEGACY S3 bucket holding pre-cutover warning/info Parquet history",
        )
        CfnOutput(
            self,
            "MetricsTableName",
            value=metrics_table.table_name,
            description="DynamoDB table holding warning/info metrics",
        )
        CfnOutput(
            self,
            "PromptLogsTableName",
            value=prompt_logs_table.table_name,
            description="DynamoDB table holding SweatAI prompt/response logs",
        )
        CfnOutput(
            self,
            "CrossAccountDynamoQueryRoleArn",
            value=cross_account_query_role.role_arn,
            description="Assume this from the tools account to Query the metrics table",
        )

    def _docker_function(
        self,
        logical_id: str,
        *,
        cmd: list[str],
        common_env: dict[str, str],
        role: iam.IRole,
        timeout_seconds: int,
        ecr_repo: ecr.IRepository,
    ) -> _lambda.DockerImageFunction:
        function_name = f"monty-{self.env_name}-{logical_id.lower()}"

        log_group = logs.LogGroup(
            self,
            f"{logical_id}LogGroup",
            log_group_name=f"/aws/lambda/{function_name}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        return _lambda.DockerImageFunction(
            self,
            logical_id,
            function_name=function_name,
            architecture=_lambda.Architecture.X86_64,
            code=_lambda.DockerImageCode.from_ecr(
                repository=ecr_repo,
                tag_or_digest=self.image_tag,
                cmd=cmd,
            ),
            timeout=Duration.seconds(timeout_seconds),
            memory_size=512,
            role=role,
            environment=common_env,
            log_group=log_group,
        )
