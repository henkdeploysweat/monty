from __future__ import annotations
import json

DBT_LAYERS = ("stg", "int", "mart", "dim", "fct", "agg", "base")
DBT_TEST_PREFIXES = ("not_null_", "unique_", "accepted_values_",
                     "relationships_", "dbt_utils_", "expect_")

def clean_and_parse_json(raw_text):
    if not raw_text:
        return {}
    if isinstance(raw_text, dict):
        return raw_text
    if isinstance(raw_text, str):
        clean_json_str = raw_text.split("||")[0].strip()
        if clean_json_str.startswith('"') and clean_json_str.endswith('"'):
            clean_json_str = clean_json_str[1:-1]
        clean_json_str = clean_json_str.replace('\\"', '"')
        try:
            return json.loads(clean_json_str)
        except json.JSONDecodeError as e:
            print("--- JSON PARSE ERROR ---")
            print(f"Error: {e}")
            raise e
    return {}

def pipeline_family(name: str, payload: str) -> str:
    dbtname = ''
    metric_name = payload.split("||")[1]
    
    # Parse payload text into a dictionary object
    tjson = clean_and_parse_json(payload)

    if name == 'dbt_run_failures':
        tt = tjson['failures'][0]['unique_id']
        parts = tt.split('.')
        dbtname = parts[1]
        name = parts[2]
        
    elif metric_name == 'dbt_model_run':
        # FIXED: Pulled unique_id safely from the root level for success runs
        tt = tjson['unique_id']
        parts = tt.split('.')
        dbtname = parts[1]
        name = parts[2]

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
        
    if len(dbtname) > 0:
        name = dbtname + "_" + name
        
    return name

# --- Test execution using your exact code variables ---
payload = """{ "compiled_sql": "with\n\nsource as (\n select * from RAW_SWEATRAN.KAYLA_PROD.survey_options\n),\n\nrenamed as (\n select\n id as survey_option_id,\n survey_id,\n position,\n created_at,\n updated_at,\n locked,\n code_name,\n _deleted as _fivetran_deleted,\n _fivetran_synced,\n qualtrics_survey_option_id\n from RAW_SWEATRAN.KAYLA_PROD.survey_options\n where coalesce(_deleted, false) = false\n)\n\nselect * from renamed", "execution_time_ms": 609, "message": "SUCCESS 1", "resource_type": "model", "rows_affected": 1, "status": "success", "thread_id": "Thread-3 (worker)", "unique_id": "model.dbt_snowflake_transformation.stg_kayla__survey_options" }"""
metric_name = "dbt_model_run"
payload = payload + "||" + metric_name
name = 'stg_kayla__user_survey_options'

name = pipeline_family(name, payload)
print(name)
payload = """{\n  \"failure_count\": 1,\n  \"failures\": [\n    {\n      \"error_message\": \"Database Error in model stg_kayla__ab_tests (models/staging/kayla/stg_kayla__ab_tests.sql)...\",\n      \"pipeline_name\": \"stg_kayla__ab_tests\",\n      \"resource_type\": \"model\",\n      \"unique_id\": \"model.dbt_snowflake_transformation.stg_kayla__ab_tests\"\n    }\n  ],\n  \"summary\": \"1 model(s) failed: stg_kayla__ab_tests\"\n}"""
name = 'dbt_run_failures'
fname = pipeline_family(name,payload)    

print(fname)
pname = pipeline_name(name,payload)    

print(pname)
# print(json.dumps(tjson, indent=4))
