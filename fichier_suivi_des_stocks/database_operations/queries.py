QUERY_UPDATE = """
DO $$

DECLARE
    target_schema TEXT := '{schema_name}';
    tbl_name TEXT;
    col_name TEXT;
    sql_cmd TEXT;

BEGIN
    FOR tbl_name, col_name IN
        SELECT
            c.table_name,
            c.column_name
        FROM information_schema.columns c
        WHERE c.table_schema = target_schema
          AND c.data_type IN ('text', 'character varying', 'real', 'double precision')
          AND c.table_name NOT LIKE 'view%' AND c.table_name <> 'etat_stock'
    LOOP
        sql_cmd := format(
            'UPDATE %I.%I SET %I = NULL WHERE %I IN (''NaN'', ''nan'');',
            target_schema,
            tbl_name,
            col_name,
            col_name
        );
        EXECUTE sql_cmd;
        RAISE NOTICE 'Colonne traitée: %.%', tbl_name, col_name;
    END LOOP;
END $$;
"""