CREATE OR REPLACE FUNCTION migration_tools.attempt_hz_date (TEXT,TEXT) RETURNS DATE AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output DATE;
    BEGIN
        FOR output IN
            EXECUTE E'SELECT (\'1970-01-01\'::date + \'' || attempt_value || E' days\'::interval)::date AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::date AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

