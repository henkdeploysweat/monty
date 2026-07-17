"""Fetch real MONITORING_METRIC log events and invoke the log_scanner Lambda.

Usage:
    python scripts/invoke_log_scanner.py \
        --log-group /aws/lambda/my-ingest-function \
        [--env dev] \
        [--hours 1] \
        [--dry-run]

--dry-run prints the event that would be sent without invoking Lambda.
"""

import argparse
import base64
import gzip
import json
import sys
import time

import boto3


REGION = "us-east-1"
FILTER_PATTERN = '{ $.MONITORING_METRIC = "*" }'


def fetch_log_events(log_group: str, hours: float) -> list[dict]:
    """Return matching log events from the last N hours."""
    client = boto3.client("logs", region_name=REGION)
    start_ms = int((time.time() - hours * 3600) * 1000)

    events = []
    kwargs = {
        "logGroupName": log_group,
        "filterPattern": FILTER_PATTERN,
        "startTime": start_ms,
    }
    while True:
        response = client.filter_log_events(**kwargs)
        events.extend(response.get("events", []))
        next_token = response.get("nextToken")
        if not next_token:
            break
        kwargs["nextToken"] = next_token

    return events


def build_cw_event(log_group: str, raw_events: list[dict]) -> dict:
    """Wrap log events in the CloudWatch Logs subscription event envelope."""
    payload = {
        "logGroup": log_group,
        "logEvents": [
            {"id": e["eventId"], "message": e["message"]}
            for e in raw_events
        ],
    }
    compressed = gzip.compress(json.dumps(payload).encode())
    return {"awslogs": {"data": base64.b64encode(compressed).decode()}}


def invoke_lambda(function_name: str, event: dict) -> dict:
    """Invoke the Lambda synchronously and return the parsed response."""
    client = boto3.client("lambda", region_name=REGION)
    response = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode(),
    )
    payload = json.loads(response["Payload"].read())
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda error: {json.dumps(payload, indent=2)}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-group", required=True,
                        help="CloudWatch log group to scan (e.g. /aws/lambda/my-function)")
    parser.add_argument("--env", default="dev", help="Monty environment (default: dev)")
    parser.add_argument("--hours", type=float, default=1.0,
                        help="How many hours back to search (default: 1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the event without invoking Lambda")
    args = parser.parse_args()

    function_name = f"monty-{args.env}-logscanner"

    print(f"Scanning {args.log_group} for the last {args.hours}h ...")
    events = fetch_log_events(args.log_group, args.hours)

    if not events:
        print("No MONITORING_METRIC events found in that window.")
        sys.exit(0)

    print(f"Found {len(events)} event(s).")
    for e in events:
        print(f"  {e['message'][:120]}")

    cw_event = build_cw_event(args.log_group, events)

    if args.dry_run:
        print("\n-- dry run: event payload --")
        print(json.dumps(cw_event, indent=2))
        sys.exit(0)

    print(f"\nInvoking {function_name} ...")
    result = invoke_lambda(function_name, cw_event)
    print(f"Result: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
