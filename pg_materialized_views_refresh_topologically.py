import argparse
import re
import psycopg2

GET_MAT_VIEWS_QUERY = """
select
    ns.nspname as mat_view_schema,
    relname as mat_view
from
    pg_class pgc
inner join pg_namespace ns on
    pgc.relnamespace = ns.oid
where
    pgc.relkind = 'm'
"""

GET_MAT_VIEWS_DEPENDENCIES_QUERY = """
select
    source_ns.nspname as source_schema,
    source_mat_view.relname as source_mat_view,
    destination_ns.nspname as dependent_schema,
    dependent_mat_view.relname as dependent_mat_view
from
    pg_depend
inner join pg_rewrite on
    pg_depend.objid = pg_rewrite.oid
inner join pg_class as dependent_mat_view on
    pg_rewrite.ev_class = dependent_mat_view.oid
inner join pg_class as source_mat_view on
    pg_depend.refobjid = source_mat_view.oid
inner join pg_namespace source_ns on
    source_ns.oid = source_mat_view.relnamespace
inner join pg_namespace destination_ns on
    destination_ns.oid = dependent_mat_view.relnamespace
inner join pg_attribute on
    pg_depend.refobjid = pg_attribute.attrelid
    and pg_depend.refobjsubid = pg_attribute.attnum
where
    dependent_mat_view.relkind = 'm'
    and source_mat_view.relkind = 'm'
    and pg_attribute.attnum > 0
group by source_mat_view, source_schema, dependent_mat_view, dependent_schema
order by source_mat_view, source_schema, dependent_mat_view, dependent_schema
"""


def main():

    parser = argparse.ArgumentParser(description='Refresh materialized views in topological order')
    parser.add_argument('--schema',
                        help='refresh all materialized views in this schema')
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='perform a trial run with no SQL statements executed')
    parser.add_argument('--include',
                        metavar='PATTERN',
                        help='only refresh materialized views that match the given pattern')
    parser.add_argument('--exclude',
                        metavar='PATTERN',
                        help='exclude materialized views that match the given pattern')
    args = parser.parse_args()
    included_schema = args.schema
    dry_run = args.dry_run
    include_pattern = args.include
    exclude_pattern = args.exclude

    def should_refresh(schema, mat_view):
        # All materialized views which obtain data from the given schema, but are not stored in
        # the given schema, are NOT updated.
        is_member_of_schema = included_schema is None or schema == included_schema
        is_included = include_pattern is None or re.match(include_pattern, mat_view)
        is_excluded = exclude_pattern is not None and re.match(exclude_pattern, mat_view)
        return is_member_of_schema and is_included and not is_excluded

    with psycopg2.connect(dsn="") as conn:

        cur = conn.cursor()
        cur.execute(GET_MAT_VIEWS_QUERY)

        all_nodes = []
        graph = {}
        for schema, mat_view, in cur.fetchall():
            all_nodes.append((schema, mat_view))
            graph[(schema, mat_view)] = []    # no incoming edges initially

        cur.execute(GET_MAT_VIEWS_DEPENDENCIES_QUERY)
        for source_schema, source_mat_view, dependent_schema, dependent_mat_view in cur.fetchall():
            graph[(dependent_schema, dependent_mat_view)].append((source_schema, source_mat_view))

        relevant_nodes = [n for n in kahn_topological_sort(graph, all_nodes) if should_refresh(*n)]
        for schema, mat_view in relevant_nodes:
            query = 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' + schema + '.' + mat_view
            print(query, flush=True)
            if not dry_run:
                try:
                    cur.execute(query)
                    conn.commit()
                except psycopg2.NotSupportedError as e:
                    if e.pgcode == '0A000' and 'concurrently' in e.pgerror.lower():
                        conn.rollback()
                        query = 'REFRESH MATERIALIZED VIEW ' + schema + '.' + mat_view
                        print("Attempt to refresh view non-concurrently")
                        print(query, flush=True)
                        cur.execute(query)
                        conn.commit()
                    else:
                        raise e



def kahn_topological_sort(graph, all_nodes):

    topological_sort_order = []
    nodes = [n for n in all_nodes if not graph[n]]

    while nodes:
        n, nodes = nodes[0], nodes[1:]
        topological_sort_order.append(n)
        reachable_from_n = [v for v in all_nodes if n in graph[v]]
        for v in reachable_from_n:
            graph[v].remove(n)
            if not graph[v]:
                nodes.append(v)

    assert not any(graph[v] for v in all_nodes)

    return topological_sort_order


if __name__ == "__main__":
    main()
