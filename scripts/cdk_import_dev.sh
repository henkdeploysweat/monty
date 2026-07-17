#!/usr/bin/env bash
# Non-interactive cdk import for monty-dev: adopts the 4 pre-existing
# Lambda log groups so the next `make cdk-deploy ENV=dev` stops failing
# with "resource already exists" on the LogGroup creates.
set -euo pipefail

REPO_ROOT="$HOME/Documents/Berg/Monty"
INFRA_DIR="$REPO_ROOT/infra"

if [ ! -s "$REPO_ROOT/.image-digest" ]; then
  echo "missing $REPO_ROOT/.image-digest — run 'make build ENV=dev' first." >&2
  exit 1
fi
DIGEST="$(cat "$REPO_ROOT/.image-digest")"
echo "using digest: $DIGEST"

# Map each CDK logical ID -> the existing log group's physical name.
# Logical IDs taken verbatim from the failed deploy's changeset.
cat > /tmp/monty_dev_import_mapping.json <<'JSON'
{
  "FailureProxyLogGroupD8470C80":  { "LogGroupName": "/aws/lambda/monty-dev-failureproxy" },
  "LogScannerLogGroupBDC2E1AA":    { "LogGroupName": "/aws/lambda/monty-dev-logscanner" },
  "ObserverLogGroup6BB4A8F8":      { "LogGroupName": "/aws/lambda/monty-dev-observer" },
  "SnsSubscriberLogGroupFF0B71F9": { "LogGroupName": "/aws/lambda/monty-dev-snssubscriber" }
}
JSON

cd "$INFRA_DIR"
cdk import monty-dev \
  -c env=dev \
  -c imageTag="$DIGEST" \
  --resource-mapping /tmp/monty_dev_import_mapping.json \
  --force
