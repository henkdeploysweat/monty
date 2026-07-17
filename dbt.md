Yes — dbt run failures still come through, for two independent reasons:

1. They're written at error severity, which is in SNOWFLAKE_SEVERITIES = ("critical", "error") — the set that still INSERTs into CUSTOM_METRICS. Only warning/info route to S3, so error failure rows are unaffected. (ADD_METRIC.md failure hook hardcodes 'error'.)
2. They bypass metric_writer entirely. The severity routing lives only in metric_writer.write() (the Lambda path). The dbt failure hook does a direct SQL INSERT INTO CUSTOM_METRICS via run_query() — it never touches metric_writer.py, so the routing doesn't even run for dbt-originated rows. The row also sets IS_ALERT=true, so the observer picks it up and posts to Slack. (Note: the dbt post_hook — not the failure hook — was separately gated to critical/error, so dbt warning/info trending metrics no longer persist; failure alerting is unaffected.)

So the severity routing poses no risk to dbt failure alerting.

One caveat worth checking, though — it's about routing, not persistence. The failure hook writes ENVIRONMENT = target.name (the second column in the INSERT, ADD_METRIC.md:518). Per the observer's routing rule, anything with ENVIRONMENT != 'prod' goes to #data-alerts-dev regardless of severity. So if your production dbt target isn't named literally prod (e.g. it's default, prod_analytics, etc.), those failure alerts will land in the dev channel, not the severity channel. Worth confirming your prod target.name is exactly prod if you expect these in the main alert channel.

Want me to check what your dbt target is named, or trace a recent pipeline_failure row to confirm where it routed?



unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN    
export AWS_PROFILE=SWEATAnalytics                                                                                                                                                                                                                                     
aws sso login --profile SWEATAnalytics
make cdk-deploy ENV=prod                                                                                                                                         

- Preview (safe, no writes):
! python3 /private/tmp/claude-504/-Users-henkduplooy-Documents-Berg-Monty/0e35b40b-6766-4ecc-a3cf-0170f7731c8e/scratchpad/compact_s3.py
- Do it:
! python3 /private/tmp/claude-504/-Users-henkduplooy-Documents-Berg-Monty/0e35b40b-6766-4ecc-a3cf-0170f7731c8e/scratchpad/compact_s3.py --apply


On it — a compaction pass that collapses each run_date=YYYYMMDD/ prefix into one file. Safe by default (preview), destructive only with --apply (writes the combined file first, then deletes the originals it read).

  Made 1 scratchpad edit +132

Done. compact_s3.py — collapses each run_date=YYYYMMDD