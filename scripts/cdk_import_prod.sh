#!/usr/bin/env bash
# Non-interactive cdk import for monty-prod: adopts the 4 pre-existing
# Lambda log groups so the next `make cdk-deploy ENV=prod` stops failing
# with "resource already exists" on the LogGroup creates.
#
# Logs are NOT touched — cdk import only adopts the existing resources
# into stack state. No data loss.
set -euo pipefail

REPO_ROOT="$HOME/Documents/Berg/Monty"
INFRA_DIR="$REPO_ROOT/infra"

if [ ! -s "$REPO_ROOT/.image-digest" ]; then
  echo "missing $REPO_ROOT/.image-digest — run 'make build ENV=prod' first." >&2
  exit 1
fi
DIGEST="$(cat "$REPO_ROOT/.image-digest")"
echo "using digest: $DIGEST"

# Map each CDK logical ID -> the existing log group's physical name.
# Logical IDs are derived from the construct path inside MontyStack, which
# is identical for dev and prod, so they match the dev ones verbatim.
cat > /tmp/monty_prod_import_mapping.json <<'JSON'
{
  "FailureProxyLogGroupD8470C80":  { "LogGroupName": "/aws/lambda/monty-prod-failureproxy" },
  "LogScannerLogGroupBDC2E1AA":    { "LogGroupName": "/aws/lambda/monty-prod-logscanner" },
  "ObserverLogGroup6BB4A8F8":      { "LogGroupName": "/aws/lambda/monty-prod-observer" },
  "SnsSubscriberLogGroupFF0B71F9": { "LogGroupName": "/aws/lambda/monty-prod-snssubscriber" }
}
JSON

cd "$INFRA_DIR"
cdk import monty-prod \
  -c env=prod \
  -c imageTag="$DIGEST" \
  --resource-mapping /tmp/monty_prod_import_mapping.json \
  --force
