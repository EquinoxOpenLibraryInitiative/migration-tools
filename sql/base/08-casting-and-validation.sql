-- remove previous version of this function
DROP FUNCTION IF EXISTS migration_tools.attempt_cast(TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION migration_tools.attempt_cast (TEXT, TEXT) RETURNS TEXT AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        datatype ALIAS FOR $2;
    BEGIN
        EXECUTE 'SELECT ' || quote_literal(attempt_value) || '::' || datatype || ' AS a;';
        RETURN attempt_value;
    EXCEPTION
        WHEN OTHERS THEN RETURN NULL;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_date (TEXT,TEXT) RETURNS DATE AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output DATE;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(REGEXP_REPLACE(attempt_value,'^(\d\d)(\d\d)(\d\d)$','\1-\2-\3')) || '::date AS a;'
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

CREATE OR REPLACE FUNCTION migration_tools.attempt_timestamptz (TEXT,TEXT) RETURNS TIMESTAMPTZ AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output TIMESTAMPTZ;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(attempt_value) || '::TIMESTAMPTZ AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::TIMESTAMPTZ AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_money (TEXT,TEXT) RETURNS NUMERIC(8,2) AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output NUMERIC(8,2);
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(REPLACE(REPLACE(attempt_value,'$',''),',','')) || '::NUMERIC(8,2) AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::NUMERIC(8,2) AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_money6 (TEXT,TEXT) RETURNS NUMERIC(6,2) AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output NUMERIC(6,2);
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(REPLACE(REPLACE(attempt_value,'$',''),',','')) || '::NUMERIC(6,2) AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::NUMERIC(6,2) AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_money_from_pennies (TEXT,TEXT) RETURNS NUMERIC(8,2) AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output NUMERIC(8,2);
    BEGIN
        IF length(regexp_replace(attempt_value,'^0+','')) > 10 THEN
            RAISE EXCEPTION 'too many digits';
        END IF;
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal((left(lpad(regexp_replace(attempt_value,'^0+',''),10,'0'),-2) || '.' || right(lpad(regexp_replace(attempt_value,'^0+',''),10,'0'),2))::numeric(8,2)) || '::NUMERIC(8,2) AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::NUMERIC(8,2) AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_money_from_pennies6 (TEXT,TEXT) RETURNS NUMERIC(6,2) AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output NUMERIC(6,2);
    BEGIN
        IF length(regexp_replace(attempt_value,'^0+','')) > 8 THEN
            RAISE EXCEPTION 'too many digits';
        END IF;
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal((left(lpad(regexp_replace(attempt_value,'^0+',''),8,'0'),-2) || '.' || right(lpad(regexp_replace(attempt_value,'^0+',''),8,'0'),2))::numeric(6,2)) || '::NUMERIC(6,2) AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::NUMERIC(6,2) AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

