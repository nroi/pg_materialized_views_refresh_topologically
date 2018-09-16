# pg_materialized_views_refresh_topologically

If your materialized views take a while to refresh, you'll probably want to refresh them regularly when the database load
is low, say every night at 3 am. Running a `REFRESH` on every materialized view every 24 hours works fine as long as all your
materialized view obtain their data only from tables. Things become slightly more complicated once they obtain their input from other materialized views: Say materialized view `B` obtains its input from materialized view `A`, and
you run a cron job each night at 3 am which refreshes all materialized views in arbitrary order. If `B` is refreshed before `A`,
the data in `B` will be at least 24 hours old immediately *after* the cron job has finished.

This python script solves this problem by representing the materialized views in a directed acyclic graph (DAG), where an incoming
edge on `B` from `A` means that materialized view `B` receives input from materialized view `A`. This graph is then topologically
sorted in order to execute all `REFRESH MATERIALIZED VIEW` statements in correct order.

### Usage
To refresh all materialized views inside a given database schema, run:

    ./pg_materialized_views_refresh_topologically <schema>

### Concurrent Refresh
This script will first attempt to refresh the materialized view [concurrently](https://www.postgresql.org/docs/10/static/sql-refreshmaterializedview.html). If that fails, it does an ordinary refresh.
