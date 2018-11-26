# pg_materialized_views_refresh_topologically

If your materialized views take a while to refresh, you'll probably want to refresh them regularly at a time when the database load
is low, say every night at 3 a.m. Running a `REFRESH` on every materialized view every 24 hours works fine as long as all your
materialized views obtain their data only from tables. Things become slightly more complicated once they obtain their data from other materialized views: Suppose materialized view `B` obtains its input from materialized view `A`, and
you run a cron job every night at 3 a.m. which refreshes all materialized views in arbitrary order. If `B` is refreshed before `A`,
the data in `B` will still be 24 hours old immediately *after* the cron job has finished.

This python script solves this problem by representing the materialized views in a directed acyclic graph (DAG), where an incoming
edge on `B` from `A` means that materialized view `B` receives input from materialized view `A`. This graph is then topologically
sorted in order to execute all `REFRESH MATERIALIZED VIEW` statements in correct order.

### Usage
To refresh all materialized views inside a given database, run:

    ./pg_materialized_views_refresh_topologically

To refresh all materialized views inside a given database schema, run:

    ./pg_materialized_views_refresh_topologically --schema <schema>

See `./pg_materialized_views_refresh_topologically -h` for more details.

Connection parameters (host, port etc.) are set with [environment variables](https://www.postgresql.org/docs/10/libpq-envars.html). The password has to be set in the [password file](https://www.postgresql.org/docs/10/libpq-pgpass.html).

### Concurrent Refresh
This script will first attempt to refresh the materialized view [concurrently](https://www.postgresql.org/docs/10/static/sql-refreshmaterializedview.html). If that fails, it does an ordinary refresh. This means you don't need to worry about error messages in the form of:
```
ERROR:  cannot refresh materialized view "my_schema.my_view" concurrently
```
