# Scheduled ingest — keep the dashboard fast

Makes the dashboard quick without changing its look: the slow `both` leg
(scanning ~34k tiny S3 Parquet files, up to 1–3 min cold) is replaced by an
indexed local SQLite read. A launchd job drains new S3 objects into the cache
every 60s and archives them, so the data stays ~1 minute fresh.

## One-time setup

1. **Log in to AWS SSO** (the ingest reads S3 with these profiles):
   ```
   aws sso login --profile SWEATAnalytics    # prod
   ```

2. **Backfill history once** (gives the timeline its 7-day baseline). This
   MOVES the consumed objects to `archive/run_date=.../` in the same bucket —
   dry-run first to see what it would touch:
   ```
   cd /Users/henkduplooy/Documents/Berg/Monty/dash
   python3 loadS3.py --env prod --start 2026-07-07 --end 2026-07-14 --ingest --dry-run
   python3 loadS3.py --env prod --start 2026-07-07 --end 2026-07-14 --ingest
   ```

3. **Switch the dashboard to the cache** — in `dash/.env`:
   ```
   export MONTY_BOTH_WARN_SOURCE=sqlite
   ```
   (Safe even before the backfill: if the cache is empty it auto-falls back to
   live S3, so the dashboard never goes blank.)

4. **Install the 60s scheduler:**
   ```
   cp scheduler/com.monty.ingest.prod.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.monty.ingest.prod.plist
   ```

## Operate

- Ingest log:            `dash/ingest.prod.log`
- launchd launch errors: `dash/ingest.launchd.log`
- Stop:   `launchctl unload ~/Library/LaunchAgents/com.monty.ingest.prod.plist`
- Start:  `launchctl load   ~/Library/LaunchAgents/com.monty.ingest.prod.plist`

## Caveats

- **SSO expires (~8–12h).** When it does, ingest ticks fail and log the auth
  error; the dashboard keeps serving the last-ingested data (just goes stale)
  until you re-run `aws sso login --profile SWEATAnalytics`.
- **Archiving drains the live partition.** After this runs, reading S3 *live*
  (`MONTY_BOTH_WARN_SOURCE=s3`) only sees objects not yet ingested — history now
  lives in SQLite + the `archive/` prefix. That is the intended trade.
- **dev too?** Copy the plist to `com.monty.ingest.dev.plist`, change the Label
  and the `prod` arg to `dev`, and `aws sso login --profile audiences-dev`. Both
  jobs share the same DB (rows are tagged by ENVIRONMENT) and the same run-lock.

## cron alternative

If you prefer cron over launchd:
```
* * * * * /Users/henkduplooy/Documents/Berg/Monty/dash/scheduler/run_ingest.sh prod
```
