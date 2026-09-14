CREATE OR REPLACE FUNCTION pg_catalog.worker_drop_shell_table_and_partition_row(relation_name text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_shell_table_and_partition_row$$;
COMMENT ON FUNCTION pg_catalog.worker_drop_shell_table_and_partition_row(text)
    IS 'drop the shell table of the given distributed table together with its pg_dist_partition row, without touching the other metadata tables';
