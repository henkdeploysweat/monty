"""Quick end-to-end smoke test of one Monty env.

Usage:
    python3 scripts/smoke_test.py dev
    python3 scripts/smoke_test.py prod
    python3 scripts/smoke_test.py dev --webhook 'https://hooks.slack.com/services/T.../B.../...'

What it does:
    1. Pulls FailureProxyUrl from the monty-<env> CloudFormation stack output.
    2. Pulls MONTY_HMAC_SECRET from monty-<env>-secrets.
    3. POSTs a unique signed test failure to /failure.
    4. Polls Snowflake (via snowsql) for the row, then for SENT_TO_SLACK=TRUE.

Requires only: aws CLI, snowsql, python3.
"""
    # body = {
    #     "pipeline_name": "monty.smoke_test",
    #     "run_id": run_id,
    #     "error_message": f"smoke test from /tmp/monty_smoke.py ({args.env})",
    #     "severity": "critical",
    #     "environment": args.env,
    # }
    
# dbt test METRIC = {
#     "id": 1330664,
#     "pipeline_name": "dbt_run_failures",
#     "metric_name": "pipeline_failure",
#     "metric_value": 1.0,
#     "severity": "error",
#     "run_id": "ec427732-fd0c-40ef-8ca8-4ae47de7816b",
#     "environment": "default",
#     "payload": {
#         "failure_count": 1,
#         "failures": [{
#             "error_message": (
#                 "Database Error in model braze_cdi_attribute_sync "
#                 "(models/marts/application/braze_cdi_attribute_sync.sql)\n"
#                 "  001003 (42000): SQL compilation error:\n"
#                 "  syntax error line 27 at position 0 unexpected 'distinct_load_hours'."
#             ),
#             "pipeline_name": "braze_cdi_attribute_sync",
#             "resource_type": "model",
#             "unique_id": "model.dbt_snowflake_transformation.braze_cdi_attribute_sync",
#         }],
#         "summary": "1 model(s) failed: braze_cdi_attribute_sync",
#     },
# }
import argparse
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid

REGION = "us-east-1"


def run(cmd):
    return subprocess.check_output(cmd, text=True).strip()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("env", choices=["dev", "prod"])
    p.add_argument("--webhook", default=None,
                   help="optional slack_webhook value to include in payload")
    args = p.parse_args()

    # 1. Resolve the failure-proxy URL from CFN outputs.
    outputs = json.loads(run([
        "aws", "cloudformation", "describe-stacks",
        "--stack-name", f"monty-{args.env}",
        "--region", REGION,
        "--query", "Stacks[0].Outputs", "--output", "json",
    ]))
    url = next(o["OutputValue"] for o in outputs if o["OutputKey"] == "FailureProxyUrl")
    print(f"failure-proxy URL: {url}")

    # 2. Resolve the HMAC secret.
    secret_str = run([
        "aws", "secretsmanager", "get-secret-value",
        "--region", REGION, "--secret-id", f"monty-{args.env}-secrets",
        "--query", "SecretString", "--output", "text",
    ])
    hmac_secret = json.loads(secret_str)["MONTY_HMAC_SECRET"]
    if not hmac_secret:
        sys.exit("MONTY_HMAC_SECRET is empty — populate the secret first.")

    # 3. Build + sign + send the payload.
    run_id = f"smoke-{uuid.uuid4().hex[:8]}"
    body = {
        "pipeline_name": "MONA LISWA ",
        "run_id": run_id,
        "error_message": """coalesce((c.plan_type != p.plan_type), true)\n                    then 'plan_type'\n                end,\n                case\n                    when coalesce(c.plan_type, p.plan_type) is not null\n                     and coalesce((c.plan_type != p.plan_type), true)\n                    then c.plan_type\n                end\n            \n                , \n                case\n                    when coalesce(c.email_subscribe, p.email_subscribe) is not null\n                     and coalesce((c.email_subscribe != p.email_subscribe), true)\n                    then 'email_subscribe'\n                end,\n                case\n                    when coalesce(c.email_subscribe, p.email_subscribe) is not null\n                     and coalesce((c.email_subscribe != p.email_subscribe), true)\n                    then c.email_subscribe\n                end\n            \n                , \n                case\n                    when coalesce(c.subscription_groups, p.subscription_groups) is not null\n                     and coalesce((c.subscription_groups != p.subscription_groups), true)\n                    then 'subscription_groups'\n                end,\n                case\n                    when coalesce(c.subscription_groups, p.subscription_groups) is not null\n                     and coalesce((c.subscription_groups != p.subscription_groups), true)\n                    then c.subscription_groups\n                end\n            \n                , \n                case\n                    when coalesce(c.ab_test_group, p.ab_test_group) is not null\n                     and coalesce((c.ab_test_group != p.ab_test_group), true)\n                    then 'ab_test_group'\n                end,\n                case\n                    when coalesce(c.ab_test_group, p.ab_test_group) is not null\n                     and coalesce((c.ab_test_group != p.ab_test_group), true)\n                    then c.ab_test_group\n                end\n            \n        ) as payload\n    from current_profiles c\n    left join previous_profiles p\n        on c.external_user_id = p.current_external_user_id\n       and c.dbt_valid_from = p.current_dbt_valid_from\n),\n\nfiltered as (\n    select\n        external_id,\n        email,\n        updated_at,\n        payload\n    from changed\n    where payload is not null\n      and payload != '{}'::variant\n),\n\nflattened as (\n    select\n        f.external_id,\n        f.email,\n        f.updated_at,\n        x.key,\n        x.value\n    from filtered f,\n    lateral flatten(input => f.payload) x\n),\n\ngrouped as (\n    select\n        external_id,\n        email,\n        updated_at,\n        object_agg(key, value) as payload\n    from flattened\n    group by external_id, email, updated_at\n)\n\nselect\n    external_id,\n    email,\n    updated_at,\n    payload\nfrom grouped\n\n),\ndeduped as (\n    select\n        external_id,\n        email,\n        updated_at,\n        payload\n    from changes\n    qualify row_number() over (\n        partition by external_id, updated_at\n        order by updated_at desc\n    ) = 1\n)\nselect\n    d.external_id,\n    nullif(d.email, '') as email,\n    d.updated_at,\n    d.payload\nfrom deduped d\nwhere d.updated_at > (\n    select coalesce(max(updated_at), '1900-01-01')\n    from DW_PROD.DBT_application.braze_cdi_attribute_sync\n)\n", "relation_name": "DW_PROD.DBT_application.braze_cdi_attribute_sync", "batch_results": null}, {"status": "skipped", "timing": [], "thread_id": "Thread-2 (worker)", "execution_time": 0.0, "adapter_response": {}, "message": null, "failures": null, "unique_id": "test.dbt_snowflake_transformation.dbt_utils_unique_combination_of_columns_braze_cdi_attribute_sync_external_id__updated_at.eda18da0d2", "compiled": false, "compiled_code": null, "relation_name": null, "batch_results": null}, {"status": "skipped", "timing": [], "thread_id": "Thread-5 (worker)", "execution_time": 0.0, "adapter_response": {}, "message": null, "failures": null, "unique_id": "test.dbt_snowflake_transformation.not_null_braze_cdi_attribute_sync_external_id.9a0b8012fa", "compiled": false, "compiled_code": null, "relation_name": null, "batch_results": null}, {"status": "skipped", "timing": [], "thread_id": "Thread-3 (worker)", "execution_time": 0.0, "adapter_response": {}, "message": null, "failures": null, "unique_id": "test.dbt_snowflake_transformation.not_null_braze_cdi_attribute_sync_payload.689a46c2c5", "compiled": false, "compiled_code": null, "relation_name": null, "batch_results": null}, {"status": "skipped", "timing": [], "thread_id": "Thread-4 (worker)", "execution_time": 0.0, "adapter_response": {}, "message": null, "failures": null, "unique_id": "test.dbt_snowflake_transformation.not_null_braze_cdi_attribute_sync_updated_at.39ecbd2b53", "compiled": false, "compiled_code": null, "relation_name": null, "batch_results": null}, {"status": "success", "timing": [{"name": "compile", "started_at": "2026-07-14T14:56:29.101701Z", "completed_at": "2026-07-14T14:56:29.855040Z"}, {"name": "execute", "started_at": "2026-07-14T14:56:29.855840Z", "completed_at": "2026-07-14T14:56:29.855847Z"}], "thread_id": "main", "execution_time": 0.754146, "adapter_response": {}, "message": "dbt_snowflake_transformation.on-run-end.0 passed", "failures": 0, "unique_id": "operation.dbt_snowflake_transformation.dbt_snowflake_transformation-on-run-end-0", "compiled": true, "compiled_code": "", "relation_name": null, "batch_results": null}, {"status": "success", "timing": [{"name": "compile", "started_at": "2026-07-14T14:56:29.856589Z", "completed_at": "2026-07-14T14:56:30.604213Z"}, {"name": "execute", "started_at": "2026-07-14T14:56:30.604913Z", "completed_at": "2026-07-14T14:56:30.604919Z"}], "thread_id": "main", "execution_time": 0.74833, "adapter_response": {}, "message": "dbt_snowflake_transformation.on-run-end.1 passed", "failures": 0, "unique_id": "operation.dbt_snowflake_transformation.dbt_snowflake_transformation-on-run-end-1", "compiled": true, "compiled_code": "", "relation_name": null, "batch_results": null}], "elapsed_time": 227.03763890266418, "args": {"log_path": "/tmp/dbt_output/logs", "invocation_command": "dbt ", "include_saved_query": false, "partial_parse_file_diff": true, "populate_cache": true, "require_nested_cumulative_type_params": false, "source_freshness_run_project_hooks": false, "send_anonymous_usage_stats": false, "log_level_file": "debug", "require_explicit_package_overrides_for_builtin_materializations": true, "require_yaml_configuration_for_mf_time_spines": false, "use_colors": true, "empty": false, "printer_width": 80, "select": ["+braze_cdi_attribute_sync"], "target": "default", "version_check": true, "state_modified_compare_more_unrendered_values": false, "warn_error_options": {"include": [], "exclude": []}, "which": "build", "log_file_max_bytes": 10485760, "static_parser": true, "cache_selected_only": false, "require_resource_names_without_spaces": false, "strict_mode": false, "state_modified_compare_vars": false, "use_colors_file": true, "export_saved_queries": false, "partial_parse": true, "exclude_resource_types": [], "log_format_file": "debug", "print": true, "introspect": true, "target_path": "/tmp/dbt_output/target/", "favor_state": false, "log_format": "default", "defer": false, "show_resource_report": false, "require_batched_execution_for_custom_microbatch_strategy": false, "skip_nodes_if_on_run_start_fails": false, "macro_debugging": false, "write_json": true, "log_level": "info", "project_dir": "/tmp/dbt", "resource_types": [], "profiles_dir": "/tmp/dbt/", "vars": {}, "quiet": false, "exclude": [], "show": false, "indirect_selection": "eager"}}""",
        "severity": "critical",
        "environment": "dev",
         "payload": {
                "failure_count": 1,
                "failures": [{
                    "error_message": (
                        "Database Error in model braze_cdi_attribute_sync "
                        "(models/marts/application/braze_cdi_attribute_sync.sql)\n"
                        "  001003 (42000): SQL compilation error: "
                        "  syntax error line 27 at position 0 unexpected 'distinct_load_hours'."
                        ),
                    "pipeline_name": "braze_cdi_attribute_sync",
                    "resource_type": "model",
                    "unique_id": "model.dbt_snowflake_transformation.braze_cdi_attribute_sync",
                }],
                "summary": "1 model(s) failed: braze_cdi_attribute_sync",
            },
     }
    if args.webhook:
        body["slack_webhook"] = args.webhook
    raw = json.dumps(body).encode("utf-8")
    sig = hmac.new(hmac_secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

    req = urllib.request.Request(
        url, data=raw, method="POST",
        headers={"Content-Type": "application/json", "X-Monty-Signature": sig},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = resp.status
        body_resp = resp.read().decode()
    print(f"POST returned {status}: {body_resp}")
    if status != 202:
        sys.exit(f"FAIL: expected 202, got {status}")

    # 4. Poll Snowflake for the row, then for delivery.
    sql = f"""
SELECT ID, PIPELINE_NAME, METRIC_NAME, SEVERITY, IS_ALERT, SENT_TO_SLACK, SENT_AT,
       PARSE_JSON(PAYLOAD):slack_webhook::string as REQUESTED_WEBHOOK
FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
WHERE RUN_ID = '{run_id}'
ORDER BY OCCURRED_AT DESC
LIMIT 1;
""".strip()

    print(f"polling Snowflake for run_id={run_id} ...")
    # Observer polls every 5 min, so allow >1 cycle for SENT_TO_SLACK to flip.
    deadline = time.time() + 360
    last = ""
    while time.time() < deadline:
        out = subprocess.run(
            ["snowsql", "-q", sql, "-o", "output_format=plain", "-o", "header=false",
             "-o", "friendly=false", "-o", "timing=false"],
            capture_output=True, text=True,
        )
        if out.returncode == 0 and out.stdout.strip():
            last = out.stdout.strip()
            if "TRUE" in last.split("\n")[-1]:  # SENT_TO_SLACK column
                print("Slack delivery confirmed:")
                print(last)
                return
        time.sleep(5)

    sys.exit(f"FAIL: row not delivered to Slack within 90s. Last:\n{last}")


if __name__ == "__main__":
    main()
