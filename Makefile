# Monty: build / test / deploy.
# `make help` lists targets.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

ENV ?= dev
PYTHON ?= python3
PIP ?= $(PYTHON) -m pip

# Per-env account/region — same accounts the ingest repos deploy into.
ifeq ($(ENV),prod)
  AWS_ACCOUNT_ID := 534977985440
else ifeq ($(ENV),dev)
  AWS_ACCOUNT_ID := 116981766237
else
  $(error Unknown ENV '$(ENV)'. Use ENV=dev or ENV=prod)
endif
AWS_REGION := us-east-1

export AWS_REGION
export AWS_DEFAULT_REGION := $(AWS_REGION)
export AWS_PAGER :=

# Shared ECR repo + image. All four Lambdas reference the same digest; only
# their `cmd` differs. `IMAGE_TAG_BUILD` is the mutable tag we push to first
# (so re-runs of `make build` work) — the immutable digest written to
# `.image-digest` is what the CDK stack actually consumes.
ECR_REPO_NAME    ?= monty
ECR_REGISTRY     := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO       := $(ECR_REGISTRY)/$(ECR_REPO_NAME)
IMAGE_TAG_BUILD  ?= latest
IMAGE_URI        := $(IMAGE_REPO):$(IMAGE_TAG_BUILD)

.PHONY: help
help:
	@echo "Targets:"
	@echo "  install        Install runtime + dev + cdk deps into the active env"
	@echo "  test           Run the pytest suite"
	@echo "  lint           Run ruff (best-effort; ignores if not installed)"
	@echo "  build          Build + push Docker image to ECR, capture digest"
	@echo "  cdk-synth      cdk synth (ENV=dev|prod)"
	@echo "  cdk-deploy     cdk deploy (ENV=dev|prod) — requires .image-digest from `make build`"
	@echo "  cdk-diff       cdk diff   (ENV=dev|prod)"
	@echo "  sql-apply      Print snowsql commands to apply DDL/proc/task in order"

.PHONY: install
install:
	$(PIP) install -r requirements.txt
	$(PIP) install -r infra/requirements.txt
	$(PIP) install pytest pytest-mock ruff

.PHONY: test
test:
	$(PYTHON) -m pytest -q tests/

.PHONY: lint
lint:
	-$(PYTHON) -m ruff check lambdas/ infra/ tests/

# ----------------------------------------------------------------------------
# ECR / Docker — pattern mirrors analytics-ingest-postgressql-repo
# ----------------------------------------------------------------------------
.PHONY: oidc-check
oidc-check:
	@aws sts get-caller-identity >/dev/null
	@aws sts get-caller-identity

.PHONY: ecr-login
ecr-login:
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(ECR_REGISTRY)

.PHONY: ecr-repo-ensure
ecr-repo-ensure:
	@set -euo pipefail; \
	if aws ecr describe-repositories --region $(AWS_REGION) --repository-names "$(ECR_REPO_NAME)" >/dev/null 2>&1; then \
	  echo "ECR repo exists: $(ECR_REPO_NAME)"; \
	else \
	  echo "Creating ECR repo: $(ECR_REPO_NAME)"; \
	  aws ecr create-repository --region $(AWS_REGION) --repository-name "$(ECR_REPO_NAME)" >/dev/null; \
	fi

.PHONY: build
build: oidc-check ecr-repo-ensure ecr-login
	DOCKER_BUILDKIT=1 docker buildx build \
	  --platform linux/amd64 \
	  -f docker/Dockerfile \
	  -t "$(IMAGE_URI)" \
	  --push --provenance=false --sbom=false .
	@# Capture the immutable ECR digest of the just-pushed image. The CDK
	@# stack reads this via `-c imageTag=<sha256:...>`. A digest changes on
	@# every push, which is what forces CFN to update the Lambda function
	@# code — a static `:latest` tag would not.
	@aws ecr describe-images \
	  --repository-name $(ECR_REPO_NAME) \
	  --image-ids imageTag=$(IMAGE_TAG_BUILD) \
	  --region $(AWS_REGION) \
	  --query 'imageDetails[0].imageDigest' \
	  --output text > .image-digest
	@echo "Pinned digest: $$(cat .image-digest)"

# ----------------------------------------------------------------------------
# CDK
# ----------------------------------------------------------------------------
.PHONY: cdk-synth
cdk-synth:
	@# Use the resolved digest if `make build` has run; otherwise a synth
	@# placeholder so local syntax checks work without a pushed image.
	@if [ -s .image-digest ]; then \
	  DIGEST="$$(cat .image-digest)"; \
	  echo "Synthing with image digest: $$DIGEST"; \
	else \
	  DIGEST="synth-placeholder"; \
	  echo "No .image-digest found — using synth-only placeholder tag."; \
	fi; \
	cd infra && cdk synth -c env=$(ENV) -c imageTag="$$DIGEST"

.PHONY: cdk-deploy
cdk-deploy: oidc-check build
	@if [ ! -s .image-digest ]; then \
	  echo ".image-digest is missing or empty. Did 'make build' succeed?"; \
	  exit 1; \
	fi
	@DIGEST="$$(cat .image-digest)"; \
	echo "Deploying with image digest: $$DIGEST"; \
	cd infra && cdk deploy -c env=$(ENV) -c imageTag="$$DIGEST" --require-approval never

.PHONY: cdk-diff
cdk-diff:
	@if [ -s .image-digest ]; then \
	  DIGEST="$$(cat .image-digest)"; \
	else \
	  DIGEST="synth-placeholder"; \
	fi; \
	cd infra && cdk diff -c env=$(ENV) -c imageTag="$$DIGEST"

.PHONY: sql-apply
sql-apply:
	@echo "# Apply DDL/proc/task in order. snowsql connection assumed configured."
	@echo "snowsql -f sql/ddl/001_database_and_schema.sql"
	@echo "snowsql -f sql/ddl/002_custom_metrics.sql"
	@echo "snowsql -f sql/ddl/003_audit_registry.sql"
	@echo "snowsql -f sql/ddl/004_alert_outbox.sql"
	@echo "snowsql -f sql/procedures/auditor_sp.sql"
	@echo "snowsql -f sql/tasks/auditor_task.sql"
	@echo "# Optional: snowsql -f sql/seed/audit_registry_examples.sql"
