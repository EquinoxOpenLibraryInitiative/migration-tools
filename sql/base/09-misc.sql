CREATE OR REPLACE FUNCTION migration_tools.is_blank (TEXT) RETURNS BOOLEAN AS $$
  BEGIN
    RETURN CASE WHEN $1 = '' THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

DROP FUNCTION IF EXISTS migration_tools.btrim_lcolumns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.btrim_lcolumns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
            AND column_name like 'l_%'
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = BTRIM(' || c_name || ')'); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.btrim_columns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.btrim_columns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = BTRIM(' || c_name || ')'); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.null_empty_lcolumns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.null_empty_lcolumns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE 
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
            AND column_name like 'l_%'
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '''' '); 
	   EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '' '' '); 
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = ''NULL'' '); 
    END LOOP;  

    RETURN TRUE;
END
$function$;

DROP FUNCTION IF EXISTS migration_tools.null_empty_columns(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.null_empty_columns(s_name TEXT, t_name TEXT) RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    c_name     TEXT;
BEGIN

    FOR c_name IN SELECT column_name FROM information_schema.columns WHERE
            table_name = t_name
            AND table_schema = s_name
            AND (data_type='text' OR data_type='character varying')
    LOOP
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '''' ');
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = '' '' ');
       EXECUTE FORMAT('UPDATE ' || s_name || '.' || t_name || ' SET ' || c_name || ' = NULL WHERE ' || c_name || ' = ''NULL'' ');
    END LOOP;

    RETURN TRUE;
END
$function$;


DROP FUNCTION IF EXISTS migration_tools.safer_truncate(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.safer_truncate(target_schema TEXT,target_table TEXT) RETURNS TEXT
 LANGUAGE plpgsql
AS $function$
DECLARE
    valid_truncate BOOLEAN DEFAULT FALSE;
    is_child_or_parent BOOLEAN DEFAULT FALSE;
    truncate_statement TEXT;
BEGIN
    -- reject if not valid
    EXECUTE 'SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)' 
        INTO valid_truncate USING target_schema, target_table;
    -- RAISE EXCEPTION 'value is %', valid_truncate;
    IF NOT valid_truncate THEN RETURN 'invalid schema or table'; END IF;
   
    -- reject if part of an evergreen schema 
    IF LOWER(target_schema) IN ('acq','action','action_trigger','actor','asset','auditor',
        'authority','biblio','booking','config','container','evergreen','extend_reporter',
        'metabib','money','oai','offline','permission','query','rating','reporter','search',
        'serial','staging','stats','unapi','url_verify','vandelay') 
    THEN RETURN 'you may not truncate a table in a production schema';
    END IF;

    -- reject if a child or parent as that gets tricky
    -- and in migrations I only truncate stand alone tables  
    SELECT 1 
    FROM pg_catalog.pg_inherits 
    WHERE inhparent::regclass::text = CONCAT_WS('.',target_schema,target_table)
        OR inhrelid::regclass::text = CONCAT_WS('.',target_schema,target_table)
    INTO is_child_or_parent;
    IF is_child_or_parent THEN RETURN 'do not truncate a parent or child table'; END IF;

    truncate_statement := 'TRUNCATE TABLE ' || target_schema || '.' || target_table;
    EXECUTE truncate_statement;
    RETURN 'success';
END
$function$;
