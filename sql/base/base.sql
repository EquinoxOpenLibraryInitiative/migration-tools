--------------------------------------------------------------------------
-- An example of how to use:
-- 
-- DROP SCHEMA foo CASCADE; CREATE SCHEMA foo; 
-- \i base.sql
-- SELECT migration_tools.init('foo');
-- SELECT migration_tools.build('foo');
-- SELECT * FROM foo.fields_requiring_mapping;
-- \d foo.actor_usr
-- create some incoming ILS specific staging tables, like CREATE foo.legacy_items ( l_barcode TEXT, .. ) INHERITS foo.asset_copy;
-- Do some mapping, like UPDATE foo.legacy_items SET barcode = TRIM(BOTH ' ' FROM l_barcode);
-- Then, to move into production, do: select migration_tools.insert_base_into_production('foo')

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

CREATE OR REPLACE FUNCTION migration_tools.country_code (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT value FROM ' || migration_schema || '.config WHERE key = ''country_code'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION migration_tools.log (TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows ALIAS FOR $3;
    BEGIN
        EXECUTE 'INSERT INTO ' || migration_schema || '.sql_log ( sql, row_count ) VALUES ( ' || quote_literal(sql) || ', ' || nrows || ' );';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.exec (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows INTEGER;
    BEGIN
        EXECUTE 'UPDATE ' || migration_schema || '.sql_current SET sql = ' || quote_literal(sql) || ';';
        --RAISE INFO '%', sql;
        EXECUTE sql;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        PERFORM migration_tools.log(migration_schema,sql,nrows);
    EXCEPTION
        WHEN OTHERS THEN 
            RAISE EXCEPTION '!!!!!!!!!!! state = %, msg = %, sql = %', SQLSTATE, SQLERRM, sql;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.debug_exec (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql ALIAS FOR $2;
        nrows INTEGER;
    BEGIN
        EXECUTE 'UPDATE ' || migration_schema || '.sql_current SET sql = ' || quote_literal(sql) || ';';
        RAISE INFO 'debug_exec sql = %', sql;
        EXECUTE sql;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        PERFORM migration_tools.log(migration_schema,sql,nrows);
    EXCEPTION
        WHEN OTHERS THEN 
            RAISE EXCEPTION '!!!!!!!!!!! state = %, msg = %, sql = %', SQLSTATE, SQLERRM, sql;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.init (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        sql TEXT;
    BEGIN
        EXECUTE 'DROP TABLE IF EXISTS ' || migration_schema || '.sql_current;';
        EXECUTE 'CREATE TABLE ' || migration_schema || '.sql_current ( sql TEXT);';
        EXECUTE 'INSERT INTO ' || migration_schema || '.sql_current ( sql ) VALUES ( '''' );';
        BEGIN
            SELECT 'CREATE TABLE ' || migration_schema || '.sql_log ( time TIMESTAMP NOT NULL DEFAULT NOW(), row_count INTEGER, sql TEXT );' INTO STRICT sql;
            EXECUTE sql;
        EXCEPTION
            WHEN OTHERS THEN 
                RAISE INFO '!!!!!!!!!!! state = %, msg = %, sql = %', SQLSTATE, SQLERRM, sql;
        END;
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.config;' );
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || '.config ( key TEXT UNIQUE, value TEXT);' );
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.copy_location,asset.copy,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,action.circulation,action.hold_request,action.hold_notification,money.grocery,money.billing,money.cash_payment,money.forgive_payment'' );' );
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''country_code'', ''USA'' );' );
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.fields_requiring_mapping;' );
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || '.fields_requiring_mapping( table_schema TEXT, table_name TEXT, column_name TEXT, data_type TEXT);' );
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.base_profile_map;' );  
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || E'.base_profile_map ( 
            id SERIAL,
            perm_grp_id INTEGER,
            transcribed_perm_group TEXT,
            legacy_field1 TEXT,
            legacy_value1 TEXT,
            legacy_field2 TEXT,
            legacy_value2 TEXT,
            legacy_field3 TEXT,
            legacy_value3 TEXT
        );' );
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''base_profile_map'', ''base_profile_map'' );' );
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.base_item_dynamic_field_map;' );  
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || E'.base_item_dynamic_field_map ( 
            id SERIAL,
            evergreen_field TEXT,
            evergreen_value TEXT,
            evergreen_datatype TEXT,
            legacy_field1 TEXT,
            legacy_value1 TEXT,
            legacy_field2 TEXT,
            legacy_value2 TEXT,
            legacy_field3 TEXT,
            legacy_value3 TEXT
        );' );
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_item_dynamic_lf1_idx ON ' || migration_schema || '.base_item_dynamic_field_map (legacy_field1,legacy_value1);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_item_dynamic_lf2_idx ON ' || migration_schema || '.base_item_dynamic_field_map (legacy_field2,legacy_value2);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_item_dynamic_lf3_idx ON ' || migration_schema || '.base_item_dynamic_field_map (legacy_field3,legacy_value3);' ); 
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''base_item_dynamic_field_map'', ''base_item_dynamic_field_map'' );' );
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.base_copy_location_map;' );  
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || E'.base_copy_location_map ( 
            id SERIAL,
            location INTEGER,
            holdable BOOLEAN NOT NULL DEFAULT TRUE,
            hold_verify BOOLEAN NOT NULL DEFAULT FALSE,
            opac_visible BOOLEAN NOT NULL DEFAULT TRUE,
            circulate BOOLEAN NOT NULL DEFAULT TRUE,
            transcribed_location TEXT,
            legacy_field1 TEXT,
            legacy_value1 TEXT,
            legacy_field2 TEXT,
            legacy_value2 TEXT,
            legacy_field3 TEXT,
            legacy_value3 TEXT
        );' );
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_base_copy_location_lf1_idx ON ' || migration_schema || '.base_copy_location_map (legacy_field1,legacy_value1);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_base_copy_location_lf2_idx ON ' || migration_schema || '.base_copy_location_map (legacy_field2,legacy_value2);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_base_copy_location_lf3_idx ON ' || migration_schema || '.base_copy_location_map (legacy_field3,legacy_value3);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_base_copy_location_loc_idx ON ' || migration_schema || '.base_copy_location_map (transcribed_location);' ); 
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''base_copy_location_map'', ''base_copy_location_map'' );' );
        PERFORM migration_tools.exec( $1, 'DROP TABLE IF EXISTS ' || migration_schema || '.base_circ_field_map;' );  
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || E'.base_circ_field_map ( 
            id SERIAL,
            circulate BOOLEAN,
            loan_period TEXT,
            max_renewals TEXT,
            max_out TEXT,
            fine_amount TEXT,
            fine_interval TEXT,
            max_fine TEXT,
            item_field1 TEXT,
            item_value1 TEXT,
            item_field2 TEXT,
            item_value2 TEXT,
            patron_field1 TEXT,
            patron_value1 TEXT,
            patron_field2 TEXT,
            patron_value2 TEXT
        );' );
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_circ_dynamic_lf1_idx ON ' || migration_schema || '.base_circ_field_map (item_field1,item_value1);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_circ_dynamic_lf2_idx ON ' || migration_schema || '.base_circ_field_map (item_field2,item_value2);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_circ_dynamic_lf3_idx ON ' || migration_schema || '.base_circ_field_map (patron_field1,patron_value1);' ); 
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_circ_dynamic_lf4_idx ON ' || migration_schema || '.base_circ_field_map (patron_field2,patron_value2);' ); 
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''base_circ_field_map'', ''base_circ_field_map'' );' );

        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_init'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_init'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        --RAISE INFO 'In migration_tools.build(%)', migration_schema;
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        PERFORM migration_tools.build_base_staging_tables(migration_schema,production_tables);
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_patron_barcode_key ON ' || migration_schema || '.actor_card ( barcode );' );
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_patron_usrname_key ON ' || migration_schema || '.actor_usr ( usrname );' );
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_copy_barcode_key ON ' || migration_schema || '.asset_copy ( barcode );' );
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_callnum_record_idx ON ' || migration_schema || '.asset_call_number ( record );' );
        PERFORM migration_tools.exec( $1, 'CREATE INDEX ' || migration_schema || '_callnum_upper_label_id_lib_idx ON ' || migration_schema || '.asset_call_number ( UPPER(label),id,owning_lib );' );
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_callnum_label_once_per_lib ON ' || migration_schema || '.asset_call_number ( record,owning_lib,label );' );
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build_base_staging_tables (TEXT,TEXT[]) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables ALIAS FOR $2;
    BEGIN
        --RAISE INFO 'In migration_tools.build_base_staging_tables(%,%)', migration_schema, production_tables;
        FOR i IN array_lower(production_tables,1) .. array_upper(production_tables,1) LOOP
            PERFORM migration_tools.build_specific_base_staging_table(migration_schema,production_tables[i]);
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
        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || '.' || base_staging_table || ' ( LIKE ' || production_table || ' INCLUDING DEFAULTS EXCLUDING CONSTRAINTS );' );
        PERFORM migration_tools.exec( $1, '
            INSERT INTO ' || migration_schema || '.fields_requiring_mapping
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = ''' || migration_schema || ''' AND table_name = ''' || base_staging_table || ''' AND is_nullable = ''NO'' AND column_default IS NULL;
        ' );
        FOR columns IN 
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = migration_schema AND table_name = base_staging_table AND is_nullable = 'NO' AND column_default IS NULL
        LOOP
            PERFORM migration_tools.exec( $1, 'ALTER TABLE ' || columns.table_schema || '.' || columns.table_name || ' ALTER COLUMN ' || columns.column_name || ' DROP NOT NULL;' );
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.insert_base_into_production (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        --RAISE INFO 'In migration_tools.insert_into_production(%)', migration_schema;
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        FOR i IN array_lower(production_tables,1) .. array_upper(production_tables,1) LOOP
            PERFORM migration_tools.insert_into_production(migration_schema,production_tables[i]);
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
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || production_table || ' SELECT * FROM ' || migration_schema || '.' || base_staging_table || ';' );
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_last_comma_prefix_first_middle_suffix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := full_name;
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR\.?\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' );
        END IF;
        IF temp ilike '%SR%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR\.?\\s*', '', 'i' );
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' );
        END IF;
        IF temp ~ E'\\sII$' THEN
            suffix := 'II';
            temp := REGEXP_REPLACE( temp, E'II$', '', 'i' );
        END IF;
        IF temp ~ E'\\sIII$' THEN
            suffix := 'III';
            temp := REGEXP_REPLACE( temp, E'III$', '', 'i' );
        END IF;
        IF temp ~ E'\\sIV$' THEN
            suffix := 'IV';
            temp := REGEXP_REPLACE( temp, E'IV$', '', 'i' );
        END IF;

        family_name := BTRIM( REGEXP_REPLACE(temp,E'^([^,]*)\\s*,.*$',E'\\1') );
        first_given_name := BTRIM( CASE WHEN temp ~ ',' THEN REGEXP_REPLACE(temp,E'^[^,]*\\s*,\\s*([^,\\s]*)\\s*.*$',E'\\1') ELSE 'N/A' END );
        second_given_name := BTRIM( CASE WHEN temp ~ ',' THEN REGEXP_REPLACE(temp,E'^[^,]*\\s*,\\s*[^,\\s]*\\s*(.*)$',E'\\1') ELSE ''  END );

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

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

CREATE OR REPLACE FUNCTION migration_tools.rebarcode (o TEXT, t BIGINT) RETURNS TEXT AS $$
    DECLARE
        n TEXT := o;
    BEGIN
        IF o ~ E'^\\d+$' AND o !~ E'^0' AND length(o) < 19 THEN -- for reference, the max value for a bigint is 9223372036854775807.  May also want to consider the case where folks want to add prefixes to non-numeric barcodes
            IF o::BIGINT < t THEN
                n = o::BIGINT + t;
            END IF;
        END IF;

        RETURN n;
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_profile_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_profile_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_item_dynamic_field_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_item_dynamic_field_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_copy_location_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_copy_location_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.base_circ_field_map (TEXT) RETURNS TEXT AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        output TEXT;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ''' || migration_schema || '.'' || value FROM ' || migration_schema || '.config WHERE key = ''base_circ_field_map'';'
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_patron_profile (TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        profile_map TEXT;
        patron_table ALIAS FOR $2;
        default_patron_profile ALIAS FOR $3;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_profile_map(migration_schema) INTO STRICT profile_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || profile_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || patron_table || ' AS u SET profile = perm_grp_id FROM ' || profile_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        PERFORM migration_tools.exec( $1, 'UPDATE ' || patron_table || ' AS u SET profile = ' || quote_literal(default_patron_profile) || ' WHERE profile IS NULL;'  );
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_patron_mapping_profile'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_patron_mapping_profile'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_item_table_dynamic (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        field_map TEXT;
        item_table ALIAS FOR $2;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_item_dynamic_field_map(migration_schema) INTO STRICT field_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || field_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || item_table || ' AS i SET ' || output.evergreen_field || E' = ' || quote_literal(output.evergreen_value) || '::' || output.evergreen_datatype || E' FROM ' || field_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_item_mapping_dynamic'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_item_mapping_dynamic'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.map_base_item_table_locations (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        base_copy_location_map TEXT;
        item_table ALIAS FOR $2;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_copy_location_map(migration_schema) INTO STRICT base_copy_location_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || base_copy_location_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || item_table || ' AS i SET location = m.location FROM ' || base_copy_location_map || ' AS m WHERE ';
            sql_where1 := NULLIF(output.legacy_field1,'') || ' = ' || quote_literal( output.legacy_value1 ) || ' AND legacy_field1 = ' || quote_literal(output.legacy_field1) || ' AND legacy_value1 = ' || quote_literal(output.legacy_value1);
            sql_where2 := NULLIF(output.legacy_field2,'') || ' = ' || quote_literal( output.legacy_value2 ) || ' AND legacy_field2 = ' || quote_literal(output.legacy_field2) || ' AND legacy_value2 = ' || quote_literal(output.legacy_value2);
            sql_where3 := NULLIF(output.legacy_field3,'') || ' = ' || quote_literal( output.legacy_value3 ) || ' AND legacy_field3 = ' || quote_literal(output.legacy_field3) || ' AND legacy_value3 = ' || quote_literal(output.legacy_value3);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_item_mapping_locations'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_item_mapping_locations'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- circulate       loan period     max renewals    max out fine amount     fine interval   max fine        item field 1    item value 1    item field 2    item value 2    patron field 1  patron value 1  patron field 2  patron value 2
CREATE OR REPLACE FUNCTION migration_tools.map_base_circ_table_dynamic (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        field_map TEXT;
        circ_table ALIAS FOR $2;
        item_table ALIAS FOR $3;
        patron_table ALIAS FOR $4;
        sql TEXT;
        sql_update TEXT;
        sql_where1 TEXT := '';
        sql_where2 TEXT := '';
        sql_where3 TEXT := '';
        sql_where4 TEXT := '';
        output RECORD;
    BEGIN
        SELECT migration_tools.base_circ_field_map(migration_schema) INTO STRICT field_map;
        FOR output IN 
            EXECUTE 'SELECT * FROM ' || field_map || E' ORDER BY id;'
        LOOP
            sql_update := 'UPDATE ' || circ_table || ' AS c SET duration = ' || quote_literal(output.loan_period) || '::INTERVAL, renewal_remaining = ' || quote_literal(output.max_renewals) || '::INTEGER, recuring_fine = ' || quote_literal(output.fine_amount) || '::NUMERIC(6,2), fine_interval = ' || quote_literal(output.fine_interval) || '::INTERVAL, max_fine = ' || quote_literal(output.max_fine) || '::NUMERIC(6,2) FROM ' || field_map || ' AS m, ' || item_table || ' AS i, ' || patron_table || ' AS u WHERE c.usr = u.id AND c.target_copy = i.id AND ';
            sql_where1 := NULLIF(output.item_field1,'') || ' = ' || quote_literal( output.item_value1 ) || ' AND item_field1 = ' || quote_literal(output.item_field1) || ' AND item_value1 = ' || quote_literal(output.item_value1);
            sql_where2 := NULLIF(output.item_field2,'') || ' = ' || quote_literal( output.item_value2 ) || ' AND item_field2 = ' || quote_literal(output.item_field2) || ' AND item_value2 = ' || quote_literal(output.item_value2);
            sql_where3 := NULLIF(output.patron_field1,'') || ' = ' || quote_literal( output.patron_value1 ) || ' AND patron_field1 = ' || quote_literal(output.patron_field1) || ' AND patron_value1 = ' || quote_literal(output.patron_value1);
            sql_where4 := NULLIF(output.patron_field2,'') || ' = ' || quote_literal( output.patron_value2 ) || ' AND patron_field2 = ' || quote_literal(output.patron_field2) || ' AND patron_value2 = ' || quote_literal(output.patron_value2);
            sql := sql_update || COALESCE(sql_where1,'') || CASE WHEN sql_where1 <> '' AND sql_where2<> ''  THEN ' AND ' ELSE '' END || COALESCE(sql_where2,'') || CASE WHEN sql_where2 <> '' AND sql_where3 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where3,'') || CASE WHEN sql_where3 <> '' AND sql_where4 <> '' THEN ' AND ' ELSE '' END || COALESCE(sql_where4,'') || ';';
            --RAISE INFO 'sql = %', sql;
            PERFORM migration_tools.exec( $1, sql );
        END LOOP;
        BEGIN
            PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''last_base_circ_field_mapping'', now() );' );
        EXCEPTION
            WHEN OTHERS THEN PERFORM migration_tools.exec( $1, 'UPDATE ' || migration_schema || '.config SET value = now() WHERE key = ''last_base_circ_field_mapping'';' );
        END;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- expand_barcode
--   $barcode      source barcode
--   $prefix       prefix to add to barcode, NULL = add no prefix
--   $maxlen       maximum length of barcode; default to 14 if left NULL
--   $pad          padding string to apply to left of source barcode before adding
--                 prefix and suffix; set to NULL or '' if no padding is desired
--   $suffix       suffix to add to barcode, NULL = add no suffix
--
-- Returns a new string consisting of prefix concatenated with padded barcode and suffix.
-- If new barcode would be longer than $maxlen, the original barcode is returned instead.
--
CREATE OR REPLACE FUNCTION migration_tools.expand_barcode (TEXT, TEXT, INTEGER, TEXT, TEXT) RETURNS TEXT AS $$
    my ($barcode, $prefix, $maxlen, $pad, $suffix) = @_;

    # default case
    return unless defined $barcode;

    $prefix     = '' unless defined $prefix;
    $maxlen ||= 14;
    $pad        = '0' unless defined $pad;
    $suffix     = '' unless defined $suffix;

    # bail out if adding prefix and suffix would bring new barcode over max length
    return $barcode if (length($prefix) + length($barcode) + length($suffix)) > $maxlen;

    my $new_barcode = $barcode;
    if ($pad ne '') {
        my $pad_length = $maxlen - length($prefix) - length($suffix);
        if (length($barcode) < $pad_length) {
            # assuming we always want padding on the left
            # also assuming that it is possible to have the pad string be longer than 1 character
            $new_barcode = substr($pad x ($pad_length - length($barcode)), 0, $pad_length - length($barcode)) . $new_barcode;
        }
    }

    # bail out if adding prefix and suffix would bring new barcode over max length
    return $barcode if (length($prefix) + length($new_barcode) + length($suffix)) > $maxlen;

    return "$prefix$new_barcode$suffix";
$$ LANGUAGE PLPERL STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_cast (TEXT,TEXT,TEXT) RETURNS RECORD AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        datatype ALIAS FOR $2;
        fail_value ALIAS FOR $3;
        output RECORD;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(attempt_value) || '::' || datatype || ' AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::' || datatype || ' AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_date (TEXT,TEXT) RETURNS DATE AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output DATE;
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(attempt_value) || '::date AS a;'
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

CREATE OR REPLACE FUNCTION migration_tools.attempt_money (TEXT,TEXT) RETURNS NUMERIC(8,2) AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output NUMERIC(8,2);
    BEGIN
        FOR output IN
            EXECUTE 'SELECT ' || quote_literal(attempt_value) || '::NUMERIC(8,2) AS a;'
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

-- add_codabar_checkdigit
--   $barcode      source barcode
--
-- If the source string is 13 or 14 characters long and contains only digits, adds or replaces the 14
-- character with a checkdigit computed according to the usual algorithm for library barcodes
-- using the Codabar symbology - see <http://www.makebarcode.com/specs/codabar.html>.  If the
-- input string does not meet those requirements, it is returned unchanged.
--
CREATE OR REPLACE FUNCTION migration_tools.add_codabar_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    return $barcode if $barcode !~ /^\d{13,14}$/;
    $barcode = substr($barcode, 0, 13); # ignore 14th digit
    my @digits = split //, $barcode;
    my $total = 0;
    $total += $digits[$_] foreach (1, 3, 5, 7, 9, 11);
    $total += (2 * $digits[$_] >= 10) ? (2 * $digits[$_] - 9) : (2 * $digits[$_]) foreach (0, 2, 4, 6, 8, 10, 12);
    my $remainder = $total % 10;
    my $checkdigit = ($remainder == 0) ? $remainder : 10 - $remainder;
    return $barcode . $checkdigit; 
$$ LANGUAGE PLPERL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_phone (TEXT,TEXT) RETURNS TEXT AS $$
  DECLARE
    phone TEXT := $1;
    areacode TEXT := $2;
    temp TEXT := '';
    output TEXT := '';
    n_digits INTEGER := 0;
  BEGIN
    temp := phone;
    temp := REGEXP_REPLACE(temp, '^1*[^0-9]*', '');
    temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2-\\3');
    n_digits := LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(temp, '(.*)?[a-zA-Z].*', E'\\1') , '[^0-9]', '', 'g'));
    IF n_digits = 7 THEN
      temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2');
      output := (areacode || '-' || temp);
    ELSE
      output := temp;
    END IF;
    RETURN output;
  END;

$$ LANGUAGE PLPGSQL STRICT VOLATILE;
