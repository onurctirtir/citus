CREATE OR REPLACE FUNCTION citus_internal.lock_pg_dist_node(lock_mode int)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_lock_pg_dist_node$$;
COMMENT ON FUNCTION citus_internal.lock_pg_dist_node(int)
    IS 'acquire a lock on pg_dist_node';
