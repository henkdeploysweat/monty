#!/usr/bin/env python3
"""CDK app entrypoint for Monty.

Mirrors the convention in analytics-ingest-iterate-repo: one stack per
environment, env name and account/region pulled from CDK context.

Synth/deploy:
    cdk deploy -c env=dev
    cdk deploy -c env=prod
"""

import os

import aws_cdk as cdk

from monty_stack import MontyStack

app = cdk.App()

env_name = app.node.try_get_context("env") or os.environ.get("MONTY_ENV", "dev")
# `imageTag` is the immutable ECR digest (sha256:...) of the just-pushed image,
# resolved by `make build` and written to `.image-digest`, then passed as
# `cdk deploy -c imageTag=<digest>` by the Makefile. The stack rejects an
# empty value so a stale-tag deploy can never silently freeze the running code.
image_tag = app.node.try_get_context("imageTag") or os.environ.get("IMAGE_TAG", "")

# Per-env account/region. dev/prod from analytics-ingest-iterate-repo CI config;
# adjust here if Monty deploys to a separate account.
_ACCOUNTS = {
    "dev":  {"account": "116981766237", "region": "us-east-1"},
    "prod": {"account": "534977985440", "region": "us-east-1"},
}

if env_name not in _ACCOUNTS:
    raise SystemExit(f"unknown env {env_name!r}; expected one of {list(_ACCOUNTS)}")

MontyStack(
    app,
    f"monty-{env_name}",
    env=cdk.Environment(**_ACCOUNTS[env_name]),
    env_name=env_name,
    image_tag=image_tag,
)

app.synth()
