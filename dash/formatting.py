from __future__ import annotations
import fnmatch
import json

DBT_LAYERS = ("stg", "int", "mart", "dim", "fct", "agg", "base","braze","funnel")
DBT_TEST_PREFIXES = ("not_null_", "unique_", "accepted_values_",
                     "relationships_", "dbt_utils_", "expect_")

# dbt projects whose model NAMES aren't distinctive on their own (e.g. appsflyer's
# `skad_redownloads`), so we prefix them with the project and group them under it.
#   unique_id 'model.appsflyer_load.skad_redownloads'
#     -> name   'appsflyer_load_skad_redownloads'
#     -> family 'appsflyer_load'
# Add a project's prefix here to give it the same treatment.
PROJECT_PREFIXED = ("appsflyer",)


def _project_prefixed(project) -> bool:
    """True if this dbt project should show model names prefixed with the project
    (and be grouped under the project as a family)."""
    return bool(project) and str(project).lower().startswith(PROJECT_PREFIXED)

# transform.py hands these functions a combined string "<payload_json>||<metric>"
# so the formatter can see both the payload and the metric name. Keep the split
# on the LAST separator, so a "||" that happens to appear inside the JSON never
# corrupts the metric name.
_SEP = "||"


def _split_payload(payload):
    """Return (json_text, metric_name) from a '<json>||<metric>' string,
    tolerating a missing separator or a non-string input."""
    if not isinstance(payload, str):
        return "", ""
    if _SEP in payload:
        json_text, metric_name = payload.rsplit(_SEP, 1)
        return json_text, metric_name
    return payload, ""


def clean_and_parse_json(raw_text):
    """Parse the JSON half of a payload into a dict.

    Returns {} for anything unparseable — a malformed payload must NEVER blow up
    the dashboard (previously this raised, which 500'd the whole page)."""
    if not raw_text:
        return {}
    if isinstance(raw_text, dict):
        return raw_text
    if not isinstance(raw_text, str):
        return {}
    clean = _split_payload(raw_text)[0].strip()
    # 1) Direct parse — Snowflake payloads are ALREADY valid JSON. (Unescaping
    #    first would corrupt any \" inside a string value, e.g. a dbt failure's
    #    error_message, and break the parse.)
    try:
        parsed = json.loads(clean)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, ValueError):
        pass
    # 2) Fallback for double-encoded payloads: some producers wrap the whole JSON
    #    in quotes and escape the inner ones. Unwrap + unescape, then retry.
    if len(clean) >= 2 and clean.startswith('"') and clean.endswith('"'):
        clean = clean[1:-1]
    clean = clean.replace('\\"', '"').replace('\\\\', '\\')
    try:
        parsed = json.loads(clean)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _dbt_identity(name, payload):
    """For dbt rows, pull (project, model) out of the payload's `unique_id`.

    * PIPELINE_NAME == 'dbt_run_failures' -> failures[0].unique_id
    * METRIC_NAME   == 'dbt_model_run'    -> root-level unique_id
    unique_id looks like '<resource>.<project>.<model>'
    (e.g. 'model.dbt_snowflake_transformation.stg_braze__email_click').

    Returns ('', '') when it isn't a dbt row or the payload can't be read — the
    caller then falls back to the raw pipeline name. Never raises."""
    _, metric_name = _split_payload(payload)
    tjson = clean_and_parse_json(payload)
    unique_id = None
    try:
        if name == 'dbt_run_failures':
            unique_id = tjson['failures'][0]['unique_id']
        elif metric_name == 'dbt_model_run':
            unique_id = tjson['unique_id']
    except (KeyError, IndexError, TypeError):
        return "", ""
    if not unique_id:
        return "", ""
    parts = str(unique_id).split('.')
    if len(parts) >= 3:
        return parts[1], parts[2]      # (project, model)
    return "", ""


def pipeline_family(name: str, payload: str = "") -> str:
    """Collapse related pipelines into one family. dbt rows are resolved to their
    real model (via the payload) first, then grouped by dbt source/layer and
    prefixed with the dbt project."""
    # Braze CDI syncs: one family, delete/attribute lanes underneath.
    if name and name.lower().startswith("braze-cdi"):
        return "braze-cdisync"

    project, model = _dbt_identity(name, payload)
    if model:
        # appsflyer-style projects group under the project name
        if _project_prefixed(project):
            return project
        name = model

    if not name:
        return "—"
    if "-ai-" in name:
        name = name.split("-ai-")[0]

    low = name.lower()
    if low.startswith(DBT_TEST_PREFIXES):
        return "dbt tests"

    if "__" in name:
        name = name.split("__")[0]

    head = name.lower().split("_")[0]
    if head in DBT_LAYERS:
        name = head

    if project:
        name = project + "_" + name

    return name


def pipeline_name(name: str, payload: str = "") -> str:
    """Display name for a pipeline. dbt rows show their real model name (from the
    payload's unique_id) instead of the generic collector name."""
    # Braze CDI syncs: the lane is just the sync type (delete / attribute).
    if name and name.lower().startswith("braze-cdi"):
        suffix = name.split("braze-cdi", 1)[1].lstrip("-_")
        return suffix or name

    project, model = _dbt_identity(name, payload)
    if model:
        # appsflyer-style projects: name is project_model, e.g.
        # appsflyer_load_skad_redownloads
        if _project_prefixed(project):
            return project + "_" + model
        return model

    if not name:
        return "—"
    # NOTE: unlike pipeline_family, pipeline_name does NOT collapse on "-ai-".
    # The lane keeps its full, distinct name so the load and the ingest stages
    # (e.g. ...-ai-load-sn vs ...-ai-ingest-sn) don't render identically — they
    # still group into one family via pipeline_family, but each row is itself.

    low = name.lower()
    if low.startswith(DBT_TEST_PREFIXES):
        return "dbt tests"

    if "__" in name:
        name = name.split("__")[0]

    head = name.lower().split("_")[0]
    if head in DBT_LAYERS:
        name = head

    return name


# ── Third-level grouping: family -> group ────────────────────────────────────
# The timeline stacks pipelines into families (level 2); `pipeline_group`
# collapses related FAMILIES into one named group (level 1). Colour-coded on the
# dashboard: same group = same colour. Everything else is a standalone group
# whose label is just the family name, simplified (drop the noisy `ai-ingest-`
# prefix / `_load` suffix): `ai-ingest-iterate` -> `iterate`,
# `ai-ingest-kaylalogs` -> `kaylalogs`, `appsflyer_load` -> `appsflyer`.
#
# Each rule is (family-name predicate -> group label). First match wins; matched
# on the family name (what `pipeline_family` returns), lower-cased.
# Labels are the FINAL display strings (already Title-Cased / hand-cased), so the
# template shows them verbatim — never re-run `prettify` on a group label.
NAMED_GROUPS = (
    (lambda f: f.startswith("dbt_snowflake_transformation"), "Dbt Snowflake Transformations"),
    (lambda f: f.startswith("sweat_analytics_core_dbt"),     "SweatAnalyticsCoreDBT"),
    (lambda f: "postgres" in f,                              "Postgres"),
    (lambda f: "plausible" in f,                             "Plausible"),
    # "cdisync" is really "CDI sync" — split + acronym-case it (exact match so
    # the braze_cdi_attribute_sync dbt model is not affected).
    (lambda f: f == "braze-cdisync",                         "Braze CDI Sync"),
)


def _simplify_family(family: str) -> str:
    """Strip the noisy affixes off a family name (no case change):
    `ai-ingest-kaylalogs` -> `kaylalogs`, `appsflyer_load` -> `appsflyer`,
    `ai-ingest-audiences` -> `audiences`. Anything else is returned unchanged."""
    f = family or ""
    low = f.lower()
    if low.startswith("ai-ingest-"):
        f = f[len("ai-ingest-"):]
    elif low.startswith("ai_ingest_"):
        f = f[len("ai_ingest_"):]
    if f.lower().endswith("_load"):
        f = f[:-len("_load")]
    return f or (family or "—")


def prettify(name) -> str:
    """Display form for a lane / family name: ONLY strip the `ai-ingest-` /
    `_load` noise — casing, underscores and dashes are left exactly as-is, so
    `ai-ingest-audiences` -> `audiences` but `braze_cdi_attribute_sync` stays
    `braze_cdi_attribute_sync`. (Group labels are cleaned separately in
    NAMED_GROUPS; family + pipeline names keep their original format.)"""
    if name is None:
        return ""
    return _simplify_family(str(name))


def _titlecase(s: str) -> str:
    """Group-label casing: `_`/`-` -> space, Title Case each word.
    `appsflyer` -> `Appsflyer`, `braze-cdisync` -> `Braze Cdisync`,
    `dbt tests` -> `Dbt Tests`. Applied to GROUP labels only — never to the
    family / pipeline names, which keep their raw format."""
    s = _simplify_family(s).replace("_", " ").replace("-", " ")
    return " ".join(w.capitalize() for w in s.split())


def pipeline_group(family: str) -> tuple[str, bool]:
    """Map a pipeline FAMILY to its GROUP (level 1). Returns (label, named).
    EVERY group label is capitalized/clean — that's the whole point of a group
    header — whether it's an explicit colour group (NAMED_GROUPS, hand-cased) or
    a standalone family promoted to its own group (Title-Cased here). The raw
    family + pipeline names underneath are left untouched.
      * named=True  -> colour group collecting several families.
      * named=False -> standalone family shown as its own (capitalized) group.
    """
    f = (family or "").lower()
    for predicate, label in NAMED_GROUPS:
        if predicate(f):
            return label, True
    return (_titlecase(family) or (family or "—")), False


# ── Hide-list ───────────────────────────────────────────────────────────────
# Put any name/pattern in here and it will NOT be shown on the dashboards
# (timeline AND anomalies). Each entry is matched (case-insensitively) against a
# pipeline's RAW name, its formatted display name, AND its family — so you can
# hide a single pipeline, a whole family, or a dbt model by whatever name you see.
#
# WILDCARDS (SQL-LIKE / glob): an entry containing `*`, `?` or `%` is treated as
# a pattern. `%` is the SQL-LIKE any-run, `*` the glob equivalent (both work),
# `?` is a single char. Examples:
#     "stg_braze__*"      hides every stg_braze source model
#     "*smoke*"           hides anything with "smoke" in the name
#     "%_snapshot"        hides every *_snapshot pipeline
REMOVE_FROM_DASHBOARD = {
    "dbt tests",
    "monty.smoke_test",
    "smoke.test",
    "assert_no_expired_members_active",
    "monty-region-migration-prod-smoke",
    "monty-smoke-test",
    "*total_row*",
    "*processing_time*",
    "*LISW*",
    "*nt_marketing__uni*"
    # stale/irregular bare dbt families removed from the board (dev + prod).
    # Only the UNPREFIXED families match — sweat_analytics_core_dbt_int /
    # dbt_snowflake_transformation_* keep their own (prefixed) names.
    "dim",
    "int",
}


def remove_from_dashboard(name) -> bool:
    """True if `name` matches any entry in REMOVE_FROM_DASHBOARD (case-insensitive)
    and should be hidden. Entries with `*`/`?`/`%` are glob/SQL-LIKE patterns;
    everything else is an exact match."""
    if not name:
        return False
    needle = str(name).strip().lower()
    for entry in REMOVE_FROM_DASHBOARD:
        pat = str(entry).strip().lower()
        if any(ch in pat for ch in "*?%"):
            if fnmatch.fnmatch(needle, pat.replace("%", "*")):
                return True
        elif needle == pat:
            return True
    return False


def kind(name: str) -> str:
    """Classify a pipeline by name into a source category used for its icon/tag:
    'aws' (lambda ingest), 'dbt' (models/tests), 'sf' (Snowflake-native), or
    'task' (everything else)."""
    if name.startswith("ai-ingest") or name.startswith("ingest") \
            or name.startswith("braze-cdi"):
        return "aws"
    if name.split("_")[0] in ("stg", "mart", "fct", "dim", "int") \
            or name.startswith("dbt"):
        return "dbt"
    if name in ("plausible", "iterate", "auditor_heartbeat") or "alarm" in name:
        return "sf"
    return "task"
