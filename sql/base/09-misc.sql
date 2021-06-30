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
