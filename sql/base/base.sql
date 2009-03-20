--------------------------------------------------------------------------
-- An example of how to use:
-- 
-- DROP SCHEMA foo CASCADE; CREATE SCHEMA foo; 
-- \i base.sql
-- SELECT migration_tools.init('foo');
-- SELECT migration_tools.build_default_base_staging_tables('foo');
-- SELECT * FROM foo.fields_requiring_mapping;
-- \d foo.actor_usr
-- create some incoming ILS specific staging tables, like CREATE foo.legacy_items ( l_barcode TEXT, .. ) INHERITS foo.asset_copy;
-- Do some mapping, like UPDATE foo.legacy_items SET barcode = TRIM(BOTH ' ' FROM l_barcode);
-- Then, to move into production, do: select migration_tools.insert_default_into_production('foo')

CREATE SCHEMA migration_tools;

CREATE OR REPLACE FUNCTION migration_tools.production_tables (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output  RECORD;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT string_to_array(value,'','') AS tables FROM ' || migration_schema || '.config WHERE key = ''production_tables'';'
        LOOP
            RETURN output.tables;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.init (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
    BEGIN
        EXECUTE 'CREATE SCHEMA ' || migration_schema || ';';
        EXECUTE 'CREATE TABLE ' || migration_schema || '.config ( key TEXT UNIQUE, value TEXT);';
        EXECUTE 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.copy_location,asset.copy,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,action.circulation,action.hold_request,money.grocery,money.billing,money.cash_payment,money.forgive_payment'' );';
        EXECUTE 'DROP TABLE IF EXISTS ' || migration_schema || '.fields_requiring_mapping;';
        EXECUTE 'CREATE TABLE ' || migration_schema || '.fields_requiring_mapping( table_schema TEXT, table_name TEXT, column_name TEXT, data_type TEXT);';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build_default_base_staging_tables (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        --RAISE INFO 'In migration_tools.build_default_base_staging_tables(%)', migration_schema;
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        EXECUTE migration_tools.build_base_staging_tables(migration_schema,production_tables);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build_base_staging_tables (TEXT,TEXT[]) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables ALIAS FOR $2;
    BEGIN
        --RAISE INFO 'In migration_tools.build_base_staging_tables(%,%)', migration_schema, production_tables;
        FOR i IN array_lower(production_tables,1) .. array_upper(production_tables,1) LOOP
            EXECUTE migration_tools.build_specific_base_staging_table(migration_schema,production_tables[i]);
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build_specific_base_staging_table (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_table ALIAS FOR $2;
        base_staging_table TEXT;
        columns RECORD;
    BEGIN
        base_staging_table = REPLACE( production_table, '.', '_' );
        --RAISE INFO 'In migration_tools.build_specific_base_staging_table(%,%) -> %', migration_schema, production_table, base_staging_table;
        EXECUTE 'CREATE TABLE ' || migration_schema || '.' || base_staging_table || ' ( like ' || production_table || ' including defaults excluding constraints );';
        EXECUTE '
            INSERT INTO ' || migration_schema || '.fields_requiring_mapping
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = ''' || migration_schema || ''' AND table_name = ''' || base_staging_table || ''' AND is_nullable = ''NO'' AND column_default IS NULL;
        ';
        FOR columns IN 
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = migration_schema AND table_name = base_staging_table AND is_nullable = 'NO' AND column_default IS NULL
        LOOP
            EXECUTE 'ALTER TABLE ' || columns.table_schema || '.' || columns.table_name || ' ALTER COLUMN ' || columns.column_name || ' DROP NOT NULL;';
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.insert_default_into_production (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        --RAISE INFO 'In migration_tools.insert_into_production(%)', migration_schema;
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        FOR i IN array_lower(production_tables,1) .. array_upper(production_tables,1) LOOP
            EXECUTE migration_tools.insert_into_production(migration_schema,production_tables[i]);
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.insert_into_production (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_table ALIAS FOR $2;
        base_staging_table TEXT;
        columns RECORD;
    BEGIN
        base_staging_table = REPLACE( production_table, '.', '_' );
        --RAISE INFO 'In migration_tools.insert_into_production(%,%) -> %', migration_schema, production_table, base_staging_table;
        EXECUTE 'INSERT INTO ' || production_table || ' SELECT * FROM ' || migration_schema || '.' || base_staging_table || ';';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.address_parse_out_citystatezip (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        city_state_zip TEXT := $1;
        city TEXT := '';
        state TEXT := '';
        zip TEXT := '';
    BEGIN
        zip := CASE WHEN city_state_zip ~ E'\\d\\d\\d\\d\\d' THEN REGEXP_REPLACE( city_state_zip, E'^.*(\\d\\d\\d\\d\\d-?\\d*).*$', E'\\1' ) ELSE '' END;
        city_state_zip := REGEXP_REPLACE( city_state_zip, E'^(.*)\\d\\d\\d\\d\\d-?\\d*(.*)$', E'\\1\\2');
        IF city_state_zip ~ ',' THEN
            state := REGEXP_REPLACE( city_state_zip, E'^(.*),(.*)$', E'\\2');
            city := REGEXP_REPLACE( city_state_zip, E'^(.*),(.*)$', E'\\1');
        ELSE
            IF city_state_zip ~ E'\\s+[A-Z][A-Z]\\s*' THEN
                state := REGEXP_REPLACE( city_state_zip, E'^.*,?\\s+([A-Z][A-Z])\\s*.*$', E'\\1' );
                city := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s+[A-Z][A-Z](\\s*.*)$', E'\\1\\2' );
            ELSE
                IF city_state_zip ~ E'^\\S+$'  THEN
                    city := city_state_zip;
                    state := 'N/A';
                ELSE
                    state := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\2');
                    city := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\1');
                END IF;
            END IF;
        END IF;
        RETURN ARRAY[ TRIM(BOTH ' ' FROM city), TRIM(BOTH ' ' FROM state), TRIM(BOTH ' ' FROM zip) ];
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

