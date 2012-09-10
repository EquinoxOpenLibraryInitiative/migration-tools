-- Copyright 2009-2012, Equinox Software, Inc.
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

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
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.copy_location,asset.copy,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,actor.usr_standing_penalty,action.circulation,action.hold_request,action.hold_notification,money.grocery,money.billing,money.cash_payment,money.forgive_payment,acq.provider,acq.provider_address,acq.provider_note,acq.fund,acq.fund_allocation,acq.fund_tag,acq.funding_source,acq.funding_source_credit,acq.lineitem,acq.purchase_order,acq.po_item,acq.invoice,acq.invoice_item,acq.invoice_entry,acq.lineitem_detail,acq.fund_debit,acq.fund_transfer,acq.po_note'' );' );
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
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_copy_id_key ON ' || migration_schema || '.asset_copy ( id );' );
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
$$ LANGUAGE PLPERLU STABLE;

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
$$ LANGUAGE PLPERLU STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_phone (TEXT,TEXT) RETURNS TEXT AS $$
  DECLARE
    phone TEXT := $1;
    areacode TEXT := $2;
    temp TEXT := '';
    output TEXT := '';
    n_digits INTEGER := 0;
  BEGIN
    temp := phone;
    temp := REGEXP_REPLACE(temp, '^1*[^0-9]*(?=[0-9])', '');
    temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2-\\3');
    n_digits := LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(temp, '(.*)?[a-zA-Z].*', E'\\1') , '[^0-9]', '', 'g'));
    IF n_digits = 7 AND areacode <> '' THEN
      temp := REGEXP_REPLACE(temp, '[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', E'\\1-\\2');
      output := (areacode || '-' || temp);
    ELSE
      output := temp;
    END IF;
    RETURN output;
  END;

$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.set_leader (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $leader = $marc->leader();
    substr($leader, $pos, 1) = $value;
    $marc->leader($leader);
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.set_008 (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $f008 = $marc->field('008');

    if ($f008) {
       my $field = $f008->data();
       substr($field, $pos, 1) = $value;
       $f008->update($field);
       $xml = $marc->as_xml_record;
       $xml =~ s/^<\?.+?\?>$//mo;
       $xml =~ s/\n//sgo;
       $xml =~ s/>\s+</></sgo;
    }
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;


CREATE OR REPLACE FUNCTION migration_tools.is_staff_profile (INT) RETURNS BOOLEAN AS $$
  DECLARE
    profile ALIAS FOR $1;
  BEGIN
    RETURN CASE WHEN 'Staff' IN (select (permission.grp_ancestors(profile)).name) THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION migration_tools.is_blank (TEXT) RETURNS BOOLEAN AS $$
  BEGIN
    RETURN CASE WHEN $1 = '' THEN TRUE ELSE FALSE END;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION migration_tools.insert_tags (TEXT, TEXT) RETURNS TEXT AS $$

  my ($marcxml, $tags) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $to_insert = MARC::Record->new_from_xml("<record>$tags</record>", 'UTF-8');

    my @incumbents = ();

    foreach my $field ( $marc->fields() ) {
      push @incumbents, $field->as_formatted();
    }

    foreach $field ( $to_insert->fields() ) {
      if (!grep {$_ eq $field->as_formatted()} @incumbents) {
        $marc->insert_fields_ordered( ($field) );
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix( tablename TEXT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix('m_pioneer.action_circulation');
--

DECLARE
  circ_lib             INT;
  target_copy          INT;
  usr                  INT;
  is_renewal           BOOLEAN;
  this_duration_rule   INT;
  this_fine_rule       INT;
  this_max_fine_rule   INT;
  rcd                  config.rule_circ_duration%ROWTYPE;
  rrf                  config.rule_recurring_fine%ROWTYPE;
  rmf                  config.rule_max_fine%ROWTYPE;
  circ                 INT;
  n                    INT := 0;
  n_circs              INT;
  
BEGIN

  EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

    -- Fetch the correct rules for this circulation
    EXECUTE ('
      SELECT
        circ_lib,
        target_copy,
        usr,
        CASE
          WHEN phone_renewal OR desk_renewal OR opac_renewal THEN TRUE
          ELSE FALSE
        END
      FROM ' || tablename || ' WHERE id = ' || circ || ';')
      INTO circ_lib, target_copy, usr, is_renewal ;
    SELECT
      INTO this_duration_rule,
           this_fine_rule,
           this_max_fine_rule
      duration_rule,
      recurring_fine_rule,
      max_fine_rule
      FROM action.item_user_circ_test(
        circ_lib,
        target_copy,
        usr,
        is_renewal
        );
    SELECT INTO rcd * FROM config.rule_circ_duration
      WHERE id = this_duration_rule;
    SELECT INTO rrf * FROM config.rule_recurring_fine
      WHERE id = this_fine_rule;
    SELECT INTO rmf * FROM config.rule_max_fine
      WHERE id = this_max_fine_rule;

    -- Apply the rules to this circulation
    EXECUTE ('UPDATE ' || tablename || ' c
    SET
      duration_rule = rcd.name,
      recurring_fine_rule = rrf.name,
      max_fine_rule = rmf.name,
      duration = rcd.normal,
      recurring_fine = rrf.normal,
      max_fine =
        CASE rmf.is_percent
          WHEN TRUE THEN (rmf.amount / 100.0) * ac.price
          ELSE rmf.amount
        END,
      renewal_remaining = rcd.max_renewals
    FROM
      config.rule_circ_duration rcd,
      config.rule_recurring_fine rrf,
      config.rule_max_fine rmf,
                        asset.copy ac
    WHERE
      rcd.id = ' || this_duration_rule || ' AND
      rrf.id = ' || this_fine_rule || ' AND
      rmf.id = ' || this_max_fine_rule || ' AND
                        ac.id = c.target_copy AND
      c.id = ' || circ || ';');

    -- Keep track of where we are in the process
    n := n + 1;
    IF (n % 100 = 0) THEN
      RAISE INFO '%', n || ' of ' || n_circs
        || ' (' || (100*n/n_circs) || '%) circs updated.';
    END IF;

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_before_20( tablename TEXT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix('m_pioneer.action_circulation');
--

DECLARE
  circ_lib             INT;
  target_copy          INT;
  usr                  INT;
  is_renewal           BOOLEAN;
  this_duration_rule   INT;
  this_fine_rule       INT;
  this_max_fine_rule   INT;
  rcd                  config.rule_circ_duration%ROWTYPE;
  rrf                  config.rule_recurring_fine%ROWTYPE;
  rmf                  config.rule_max_fine%ROWTYPE;
  circ                 INT;
  n                    INT := 0;
  n_circs              INT;
  
BEGIN

  EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

    -- Fetch the correct rules for this circulation
    EXECUTE ('
      SELECT
        circ_lib,
        target_copy,
        usr,
        CASE
          WHEN phone_renewal OR desk_renewal OR opac_renewal THEN TRUE
          ELSE FALSE
        END
      FROM ' || tablename || ' WHERE id = ' || circ || ';')
      INTO circ_lib, target_copy, usr, is_renewal ;
    SELECT
      INTO this_duration_rule,
           this_fine_rule,
           this_max_fine_rule
      duration_rule,
      recuring_fine_rule,
      max_fine_rule
      FROM action.find_circ_matrix_matchpoint(
        circ_lib,
        target_copy,
        usr,
        is_renewal
        );
    SELECT INTO rcd * FROM config.rule_circ_duration
      WHERE id = this_duration_rule;
    SELECT INTO rrf * FROM config.rule_recurring_fine
      WHERE id = this_fine_rule;
    SELECT INTO rmf * FROM config.rule_max_fine
      WHERE id = this_max_fine_rule;

    -- Apply the rules to this circulation
    EXECUTE ('UPDATE ' || tablename || ' c
    SET
      duration_rule = rcd.name,
      recuring_fine_rule = rrf.name,
      max_fine_rule = rmf.name,
      duration = rcd.normal,
      recuring_fine = rrf.normal,
      max_fine =
        CASE rmf.is_percent
          WHEN TRUE THEN (rmf.amount / 100.0) * ac.price
          ELSE rmf.amount
        END,
      renewal_remaining = rcd.max_renewals
    FROM
      config.rule_circ_duration rcd,
      config.rule_recuring_fine rrf,
      config.rule_max_fine rmf,
                        asset.copy ac
    WHERE
      rcd.id = ' || this_duration_rule || ' AND
      rrf.id = ' || this_fine_rule || ' AND
      rmf.id = ' || this_max_fine_rule || ' AND
                        ac.id = c.target_copy AND
      c.id = ' || circ || ';');

    -- Keep track of where we are in the process
    n := n + 1;
    IF (n % 100 = 0) THEN
      RAISE INFO '%', n || ' of ' || n_circs
        || ' (' || (100*n/n_circs) || '%) circs updated.';
    END IF;

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_after_20( tablename TEXT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix('m_pioneer.action_circulation');
--

DECLARE
  circ_lib             INT;
  target_copy          INT;
  usr                  INT;
  is_renewal           BOOLEAN;
  this_duration_rule   INT;
  this_fine_rule       INT;
  this_max_fine_rule   INT;
  rcd                  config.rule_circ_duration%ROWTYPE;
  rrf                  config.rule_recurring_fine%ROWTYPE;
  rmf                  config.rule_max_fine%ROWTYPE;
  circ                 INT;
  n                    INT := 0;
  n_circs              INT;
  
BEGIN

  EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

    -- Fetch the correct rules for this circulation
    EXECUTE ('
      SELECT
        circ_lib,
        target_copy,
        usr,
        CASE
          WHEN phone_renewal OR desk_renewal OR opac_renewal THEN TRUE
          ELSE FALSE
        END
      FROM ' || tablename || ' WHERE id = ' || circ || ';')
      INTO circ_lib, target_copy, usr, is_renewal ;
    SELECT
      INTO this_duration_rule,
           this_fine_rule,
           this_max_fine_rule
      (matchpoint).duration_rule,
      (matchpoint).recurring_fine_rule,
      (matchpoint).max_fine_rule
      FROM action.find_circ_matrix_matchpoint(
        circ_lib,
        target_copy,
        usr,
        is_renewal
        );
    SELECT INTO rcd * FROM config.rule_circ_duration
      WHERE id = this_duration_rule;
    SELECT INTO rrf * FROM config.rule_recurring_fine
      WHERE id = this_fine_rule;
    SELECT INTO rmf * FROM config.rule_max_fine
      WHERE id = this_max_fine_rule;

    -- Apply the rules to this circulation
    EXECUTE ('UPDATE ' || tablename || ' c
    SET
      duration_rule = rcd.name,
      recurring_fine_rule = rrf.name,
      max_fine_rule = rmf.name,
      duration = rcd.normal,
      recurring_fine = rrf.normal,
      max_fine =
        CASE rmf.is_percent
          WHEN TRUE THEN (rmf.amount / 100.0) * ac.price
          ELSE rmf.amount
        END,
      renewal_remaining = rcd.max_renewals,
      grace_period = rrf.grace_period
    FROM
      config.rule_circ_duration rcd,
      config.rule_recurring_fine rrf,
      config.rule_max_fine rmf,
                        asset.copy ac
    WHERE
      rcd.id = ' || this_duration_rule || ' AND
      rrf.id = ' || this_fine_rule || ' AND
      rmf.id = ' || this_max_fine_rule || ' AND
                        ac.id = c.target_copy AND
      c.id = ' || circ || ';');

    -- Keep track of where we are in the process
    n := n + 1;
    IF (n % 100 = 0) THEN
      RAISE INFO '%', n || ' of ' || n_circs
        || ' (' || (100*n/n_circs) || '%) circs updated.';
    END IF;

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION migration_tools.stage_not_applicable_asset_stat_cats( schemaname TEXT ) RETURNS VOID AS $$

-- USAGE: Make sure the stat_cat and stat_cat_entry tables are populated, including exactly one 'Not Applicable' entry per stat cat.
--        Then SELECT migration_tools.stage_not_applicable_asset_stat_cats('m_foo');

-- TODO: Make a variant that will go directly to production tables -- which would be useful for retrofixing the absence of N/A cats.
-- TODO: Add a similar tool for actor stat cats, which behave differently.

DECLARE
	c                    TEXT := schemaname || '.asset_copy_legacy';
	sc									 TEXT := schemaname || '.asset_stat_cat';
	sce									 TEXT := schemaname || '.asset_stat_cat_entry';
	scecm								 TEXT := schemaname || '.asset_stat_cat_entry_copy_map';
	stat_cat						 INT;
  stat_cat_entry       INT;
  
BEGIN

  FOR stat_cat IN EXECUTE ('SELECT id FROM ' || sc) LOOP

		EXECUTE ('SELECT id FROM ' || sce || ' WHERE stat_cat = ' || stat_cat || E' AND value = \'Not Applicable\';') INTO stat_cat_entry;

		EXECUTE ('INSERT INTO ' || scecm || ' (owning_copy, stat_cat, stat_cat_entry)
							SELECT c.id, ' || stat_cat || ', ' || stat_cat_entry || ' FROM ' || c || ' c WHERE c.id NOT IN
							(SELECT owning_copy FROM ' || scecm || ' WHERE stat_cat = ' || stat_cat || ');');

  END LOOP;

  RETURN;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.assign_standing_penalties ( ) RETURNS VOID AS $$

-- USAGE: Once circulation data has been loaded, and group penalty thresholds have been set up, run this.
--        This will assign standing penalties as needed.

DECLARE
  org_unit  INT;
  usr       INT;

BEGIN

  FOR org_unit IN EXECUTE ('SELECT DISTINCT org_unit FROM permission.grp_penalty_threshold;') LOOP

    FOR usr IN EXECUTE ('SELECT id FROM actor.usr WHERE NOT deleted;') LOOP
  
      EXECUTE('SELECT actor.calculate_system_penalties(' || usr || ', ' || org_unit || ');');

    END LOOP;

  END LOOP;

  RETURN;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.insert_metarecords_for_pristine_database () RETURNS VOID AS $$

BEGIN
  INSERT INTO metabib.metarecord (fingerprint, master_record)
    SELECT  DISTINCT ON (b.fingerprint) b.fingerprint, b.id
      FROM  biblio.record_entry b
      WHERE NOT b.deleted
        AND b.id IN (SELECT r.id FROM biblio.record_entry r LEFT JOIN metabib.metarecord_source_map k ON (k.source = r.id) WHERE k.id IS NULL AND r.fingerprint IS NOT NULL)
        AND NOT EXISTS ( SELECT 1 FROM metabib.metarecord WHERE fingerprint = b.fingerprint )
      ORDER BY b.fingerprint, b.quality DESC;
  INSERT INTO metabib.metarecord_source_map (metarecord, source)
    SELECT  m.id, r.id
      FROM  biblio.record_entry r
      JOIN  metabib.metarecord m USING (fingerprint)
     WHERE  NOT r.deleted;
END;
  
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.insert_metarecords_for_incumbent_database () RETURNS VOID AS $$

BEGIN
  INSERT INTO metabib.metarecord (fingerprint, master_record)
    SELECT  DISTINCT ON (b.fingerprint) b.fingerprint, b.id
      FROM  biblio.record_entry b
      WHERE NOT b.deleted
        AND b.id IN (SELECT r.id FROM biblio.record_entry r LEFT JOIN metabib.metarecord_source_map k ON (k.source = r.id) WHERE k.id IS NULL AND r.fingerprint IS NOT NULL)
        AND NOT EXISTS ( SELECT 1 FROM metabib.metarecord WHERE fingerprint = b.fingerprint )
      ORDER BY b.fingerprint, b.quality DESC;
  INSERT INTO metabib.metarecord_source_map (metarecord, source)
    SELECT  m.id, r.id
      FROM  biblio.record_entry r
        JOIN metabib.metarecord m USING (fingerprint)
      WHERE NOT r.deleted
        AND r.id IN (SELECT b.id FROM biblio.record_entry b LEFT JOIN metabib.metarecord_source_map k ON (k.source = b.id) WHERE k.id IS NULL);
END;
    
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.create_cards( schemaname TEXT ) RETURNS VOID AS $$

-- USAGE: Make sure the patrons are staged in schemaname.actor_usr_legacy and have 'usrname' assigned.
--        Then SELECT migration_tools.create_cards('m_foo');

DECLARE
	u                    TEXT := schemaname || '.actor_usr_legacy';
	c                    TEXT := schemaname || '.actor_card';
  
BEGIN

	EXECUTE ('DELETE FROM ' || c || ';');
	EXECUTE ('INSERT INTO ' || c || ' (usr, barcode) SELECT id, usrname FROM ' || u || ';');
	EXECUTE ('UPDATE ' || u || ' u SET card = c.id FROM ' || c || ' c WHERE c.usr = u.id;');

  RETURN;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.insert_856_9 (TEXT, TEXT) RETURNS TEXT AS $$

  ## USAGE: UPDATE biblio.record_entry SET marc = migration_tools.insert_856_9(marc, 'ABC') WHERE [...];

  my ($marcxml, $shortname) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ( $marc->field('856') ) {
      if ( scalar(grep( /(contentreserve|netlibrary|overdrive)\.com/i, $field->subfield('u'))) > 0 &&
           ! ( $field->as_string('9') =~ m/$shortname/ ) ) {
        $field->add_subfields( '9' => $shortname );
				$field->update( ind2 => '0');
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.change_call_number(copy_id BIGINT, new_label TEXT, cn_class BIGINT) RETURNS VOID AS $$

DECLARE
  old_volume   BIGINT;
  new_volume   BIGINT;
  bib          BIGINT;
  owner        INTEGER;
  old_label    TEXT;
  remainder    BIGINT;

BEGIN

  -- Bail out if asked to change the label to ##URI##
  IF new_label = '##URI##' THEN
    RETURN;
  END IF;

  -- Gather information
  SELECT call_number INTO old_volume FROM asset.copy WHERE id = copy_id;
  SELECT record INTO bib FROM asset.call_number WHERE id = old_volume;
  SELECT owning_lib, label INTO owner, old_label FROM asset.call_number WHERE id = old_volume;

  -- Bail out if the label already is ##URI##
  IF old_label = '##URI##' THEN
    RETURN;
  END IF;

  -- Bail out if the call number label is already correct
  IF new_volume = old_volume THEN
    RETURN;
  END IF;

  -- Check whether we already have a destination volume available
  SELECT id INTO new_volume FROM asset.call_number 
    WHERE 
      record = bib AND
      owning_lib = owner AND
      label = new_label AND
      NOT deleted;

  -- Create destination volume if needed
  IF NOT FOUND THEN
    INSERT INTO asset.call_number (creator, editor, record, owning_lib, label, label_class) 
      VALUES (1, 1, bib, owner, new_label, cn_class);
    SELECT id INTO new_volume FROM asset.call_number
      WHERE 
        record = bib AND
        owning_lib = owner AND
        label = new_label AND
        NOT deleted;
  END IF;

  -- Move copy to destination
  UPDATE asset.copy SET call_number = new_volume WHERE id = copy_id;

  -- Delete source volume if it is now empty
  SELECT id INTO remainder FROM asset.copy WHERE call_number = old_volume AND NOT deleted;
  IF NOT FOUND THEN
    DELETE FROM asset.call_number WHERE id = old_volume;
  END IF;

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.zip_to_city_state_county (TEXT) RETURNS TEXT[] AS $$

	my $input = $_[0];
	my %zipdata;

	open (FH, '<', '/openils/var/data/zips.txt') or return ('No File Found', 'No File Found', 'No File Found');

	while (<FH>) {
		chomp;
		my ($junk, $state, $city, $zip, $foo, $bar, $county, $baz, $morejunk) = split(/\|/);
		$zipdata{$zip} = [$city, $state, $county];
	}

	if (defined $zipdata{$input}) {
		my ($city, $state, $county) = @{$zipdata{$input}};
		return [$city, $state, $county];
	} elsif (defined $zipdata{substr $input, 0, 5}) {
		my ($city, $state, $county) = @{$zipdata{substr $input, 0, 5}};
		return [$city, $state, $county];
	} else {
		return ['ZIP not found', 'ZIP not found', 'ZIP not found'];
	}
  
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.check_ou_depth ( ) RETURNS VOID AS $$

DECLARE
  ou  INT;
	org_unit_depth INT;
	ou_parent INT;
	parent_depth INT;
  errors_found BOOLEAN;
	ou_shortname TEXT;
	parent_shortname TEXT;
	ou_type_name TEXT;
	parent_type TEXT;
	type_id INT;
	type_depth INT;
	type_parent INT;
	type_parent_depth INT;
	proper_parent TEXT;

BEGIN

	errors_found := FALSE;

-- Checking actor.org_unit_type

	FOR type_id IN EXECUTE ('SELECT id FROM actor.org_unit_type ORDER BY id;') LOOP

		SELECT depth FROM actor.org_unit_type WHERE id = type_id INTO type_depth;
		SELECT parent FROM actor.org_unit_type WHERE id = type_id INTO type_parent;

		IF type_parent IS NOT NULL THEN

			SELECT depth FROM actor.org_unit_type WHERE id = type_parent INTO type_parent_depth;

			IF type_depth - type_parent_depth <> 1 THEN
				SELECT name FROM actor.org_unit_type WHERE id = type_id INTO ou_type_name;
				SELECT name FROM actor.org_unit_type WHERE id = type_parent INTO parent_type;
				RAISE INFO 'The % org unit type has a depth of %, but its parent org unit type, %, has a depth of %.',
					ou_type_name, type_depth, parent_type, type_parent_depth;
				errors_found := TRUE;

			END IF;

		END IF;

	END LOOP;

-- Checking actor.org_unit

  FOR ou IN EXECUTE ('SELECT id FROM actor.org_unit ORDER BY shortname;') LOOP

		SELECT parent_ou FROM actor.org_unit WHERE id = ou INTO ou_parent;
		SELECT t.depth FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou INTO org_unit_depth;
		SELECT t.depth FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou_parent INTO parent_depth;
		SELECT shortname FROM actor.org_unit WHERE id = ou INTO ou_shortname;
		SELECT shortname FROM actor.org_unit WHERE id = ou_parent INTO parent_shortname;
		SELECT t.name FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou INTO ou_type_name;
		SELECT t.name FROM actor.org_unit_type t, actor.org_unit o WHERE o.ou_type = t.id and o.id = ou_parent INTO parent_type;

		IF ou_parent IS NOT NULL THEN

			IF	(org_unit_depth - parent_depth <> 1) OR (
				(SELECT parent FROM actor.org_unit_type WHERE name = ou_type_name) <> (SELECT id FROM actor.org_unit_type WHERE name = parent_type)
			) THEN
				RAISE INFO '% (org unit %) is a % (depth %) but its parent, % (org unit %), is a % (depth %).', 
					ou_shortname, ou, ou_type_name, org_unit_depth, parent_shortname, ou_parent, parent_type, parent_depth;
				errors_found := TRUE;
			END IF;

		END IF;

  END LOOP;

	IF NOT errors_found THEN
		RAISE INFO 'No errors found.';
	END IF;

  RETURN;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.refresh_opac_visible_copies ( ) RETURNS VOID AS $$

BEGIN	

	DELETE FROM asset.opac_visible_copies;

	INSERT INTO asset.opac_visible_copies (id, circ_lib, record)
		SELECT DISTINCT
			cp.id, cp.circ_lib, cn.record
		FROM
			asset.copy cp
			JOIN asset.call_number cn ON (cn.id = cp.call_number)
			JOIN actor.org_unit a ON (cp.circ_lib = a.id)
			JOIN asset.copy_location cl ON (cp.location = cl.id)
			JOIN config.copy_status cs ON (cp.status = cs.id)
			JOIN biblio.record_entry b ON (cn.record = b.id)
		WHERE 
			NOT cp.deleted AND
			NOT cn.deleted AND
			NOT b.deleted AND
			cs.opac_visible AND
			cl.opac_visible AND
			cp.opac_visible AND
			a.opac_visible AND
			cp.id NOT IN (SELECT id FROM asset.opac_visible_copies);

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.change_owning_lib(copy_id BIGINT, new_owning_lib INTEGER) RETURNS VOID AS $$

DECLARE
  old_volume     BIGINT;
  new_volume     BIGINT;
  bib            BIGINT;
  old_owning_lib INTEGER;
	old_label      TEXT;
  remainder      BIGINT;

BEGIN

  -- Gather information
  SELECT call_number INTO old_volume FROM asset.copy WHERE id = copy_id;
  SELECT record INTO bib FROM asset.call_number WHERE id = old_volume;
  SELECT owning_lib, label INTO old_owning_lib, old_label FROM asset.call_number WHERE id = old_volume;

	-- Bail out if the new_owning_lib is not the ID of an org_unit
	IF new_owning_lib NOT IN (SELECT id FROM actor.org_unit) THEN
		RAISE WARNING 
			'% is not a valid actor.org_unit ID; no change made.', 
				new_owning_lib;
		RETURN;
	END IF;

  -- Bail out discreetly if the owning_lib is already correct
  IF new_owning_lib = old_owning_lib THEN
    RETURN;
  END IF;

  -- Check whether we already have a destination volume available
  SELECT id INTO new_volume FROM asset.call_number 
    WHERE 
      record = bib AND
      owning_lib = new_owning_lib AND
      label = old_label AND
      NOT deleted;

  -- Create destination volume if needed
  IF NOT FOUND THEN
    INSERT INTO asset.call_number (creator, editor, record, owning_lib, label) 
      VALUES (1, 1, bib, new_owning_lib, old_label);
    SELECT id INTO new_volume FROM asset.call_number
      WHERE 
        record = bib AND
        owning_lib = new_owning_lib AND
        label = old_label AND
        NOT deleted;
  END IF;

  -- Move copy to destination
  UPDATE asset.copy SET call_number = new_volume WHERE id = copy_id;

  -- Delete source volume if it is now empty
  SELECT id INTO remainder FROM asset.copy WHERE call_number = old_volume AND NOT deleted;
  IF NOT FOUND THEN
    DELETE FROM asset.call_number WHERE id = old_volume;
  END IF;

END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migration_tools.change_owning_lib(copy_id BIGINT, new_owner TEXT) RETURNS VOID AS $$

-- You can use shortnames with this function, which looks up the org unit ID and passes it to change_owning_lib(BIGINT,INTEGER).

DECLARE
	new_owning_lib	INTEGER;

BEGIN

	-- Parse the new_owner as an org unit ID or shortname
	IF new_owner IN (SELECT shortname FROM actor.org_unit) THEN
		SELECT id INTO new_owning_lib FROM actor.org_unit WHERE shortname = new_owner;
		PERFORM migration_tools.change_owning_lib(copy_id, new_owning_lib);
	ELSIF new_owner ~ E'^[0-9]+$' THEN
		IF new_owner::INTEGER IN (SELECT id FROM actor.org_unit) THEN
			RAISE INFO 
				'%',
				E'You don\'t need to put the actor.org_unit ID in quotes; '
					|| E'if you put it in quotes, I\'m going to try to parse it as a shortname first.';
			new_owning_lib := new_owner::INTEGER;
		PERFORM migration_tools.change_owning_lib(copy_id, new_owning_lib);
		END IF;
	ELSE
		RAISE WARNING 
			'% is not a valid actor.org_unit shortname or ID; no change made.', 
			new_owning_lib;
		RETURN;
	END IF;

END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.marc_parses( TEXT ) RETURNS BOOLEAN AS $func$

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $xml = shift;

eval { my $r = MARC::Record->new_from_xml( $xml ); };
if ($@) {
    return 0;
} else {
    return 1;
}

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.marc_parses(TEXT) IS 'Return boolean indicating if MARCXML string is parseable by MARC::File::XML';

CREATE OR REPLACE FUNCTION migration_tools.simple_export_library_config(dir TEXT, orgs INT[]) RETURNS VOID AS $FUNC$
BEGIN
   EXECUTE $$COPY (SELECT * FROM actor.hours_of_operation WHERE id IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_hours_of_operation'$$;
   EXECUTE $$COPY (SELECT org_unit, close_start, close_end, reason FROM actor.org_unit_closed WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_org_unit_closed'$$;
   EXECUTE $$COPY (SELECT org_unit, name, value FROM actor.org_unit_setting WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/actor_org_unit_setting'$$;
   EXECUTE $$COPY (SELECT name, owning_lib, holdable, hold_verify, opac_visible, circulate FROM asset.copy_location WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_copy_location'$$;
   EXECUTE $$COPY (SELECT grp, org_unit, penalty, threshold FROM permission.grp_penalty_threshold WHERE org_unit IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/permission_grp_penalty_threshold'$$;
   EXECUTE $$COPY (SELECT owning_lib, label, label_sortkey FROM asset.call_number_prefix WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_call_number_prefix'$$;
   EXECUTE $$COPY (SELECT owning_lib, label, label_sortkey FROM asset.call_number_suffix WHERE owning_lib IN ($$ ||
           ARRAY_TO_STRING(orgs, ',') || $$)$$ ||
           $$) TO '$$ ||  dir || $$/asset_call_number_suffix'$$;
   EXECUTE $$COPY config.rule_circ_duration TO '$$ ||  dir || $$/config_rule_circ_duration'$$;
   EXECUTE $$COPY config.rule_age_hold_protect TO '$$ ||  dir || $$/config_rule_age_hold_protect'$$;
   EXECUTE $$COPY config.rule_max_fine TO '$$ ||  dir || $$/config_rule_max_fine'$$;
   EXECUTE $$COPY config.rule_recurring_fine TO '$$ ||  dir || $$/config_rule_recurring_fine'$$;
   EXECUTE $$COPY permission.grp_tree TO '$$ ||  dir || $$/permission_grp_tree'$$;
END;
$FUNC$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.simple_import_library_config(dir TEXT) RETURNS VOID AS $FUNC$
BEGIN
   EXECUTE $$COPY actor.hours_of_operation FROM '$$ ||  dir || $$/actor_hours_of_operation'$$;
   EXECUTE $$COPY actor.org_unit_closed (org_unit, close_start, close_end, reason) FROM '$$ ||  dir || $$/actor_org_unit_closed'$$;
   EXECUTE $$COPY actor.org_unit_setting (org_unit, name, value) FROM '$$ ||  dir || $$/actor_org_unit_setting'$$;
   EXECUTE $$COPY asset.copy_location (name, owning_lib, holdable, hold_verify, opac_visible, circulate) FROM '$$ ||  dir || $$/asset_copy_location'$$;
   EXECUTE $$COPY permission.grp_penalty_threshold (grp, org_unit, penalty, threshold) FROM '$$ ||  dir || $$/permission_grp_penalty_threshold'$$;
   EXECUTE $$COPY asset.call_number_prefix (owning_lib, label, label_sortkey) FROM '$$ ||  dir || $$/asset_call_number_prefix'$$;
   EXECUTE $$COPY asset.call_number_suffix (owning_lib, label, label_sortkey) FROM '$$ ||  dir || $$/asset_call_number_suffix'$$;

   -- import any new circ rules
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_circ_duration', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_age_hold_protect', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_max_fine', 'id', 'name');
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'config', 'rule_recurring_fine', 'id', 'name');

   -- and permission groups
   PERFORM migration_tools.simple_import_new_rows_by_value(dir, 'permission', 'grp_tree', 'id', 'name');

END;
$FUNC$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration_tools.simple_import_new_rows_by_value(dir TEXT, schemaname TEXT, tablename TEXT, idcol TEXT, matchcol TEXT) RETURNS VOID AS $FUNC$
DECLARE
    name TEXT;
    loopq TEXT;
    existsq TEXT;
    ct INTEGER;
    cols TEXT[];
    copyst TEXT;
BEGIN
    EXECUTE $$DROP TABLE IF EXISTS tmp_$$ || tablename;
    EXECUTE $$CREATE TEMPORARY TABLE tmp_$$ || tablename || $$ AS SELECT * FROM $$ || schemaname || '.' || tablename || $$ LIMIT 0$$;
    EXECUTE $$COPY tmp_$$ || tablename || $$ FROM '$$ ||  dir || '/' || schemaname || '_' || tablename || $$'$$;
    loopq := 'SELECT ' || matchcol || ' FROM tmp_' || tablename || ' ORDER BY ' || idcol;
    existsq := 'SELECT COUNT(*) FROM ' || schemaname || '.' || tablename || ' WHERE ' || matchcol || ' = $1';
    SELECT ARRAY_AGG(column_name::TEXT) INTO cols FROM information_schema.columns WHERE table_schema = schemaname AND table_name = tablename AND column_name <> idcol;
    FOR name IN EXECUTE loopq LOOP
       EXECUTE existsq INTO ct USING name;
       IF ct = 0 THEN
           RAISE NOTICE 'inserting %.% row for %', schemaname, tablename, name;
           copyst := 'INSERT INTO ' || schemaname || '.' || tablename || ' (' || ARRAY_TO_STRING(cols, ',') || ') SELECT ' || ARRAY_TO_STRING(cols, ',') || 
                     ' FROM tmp_' || tablename || ' WHERE ' || matchcol || ' = $1';
           EXECUTE copyst USING name;
       END IF;
    END LOOP;
END;
$FUNC$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $target_xml = shift;
my $source_xml = shift;
my $tags = shift;

my $target;
my $source;

eval { $target = MARC::Record->new_from_xml( $target_xml ); };
if ($@) {
    return;
}
eval { $source = MARC::Record->new_from_xml( $source_xml ); };
if ($@) {
    return;
}

my $source_id = $source->subfield('901', 'c');
$source_id = $source->subfield('903', 'a') unless $source_id;
my $target_id = $target->subfield('901', 'c');
$target_id = $target->subfield('903', 'a') unless $target_id;

my %existing_fields;
foreach my $tag (@$tags) {
    my %existing_fields = map { $_->as_formatted() => 1 } $target->field($tag);
    my @to_add = grep { not exists $existing_fields{$_->as_formatted()} } $source->field($tag);
    $target->insert_fields_ordered(map { $_->clone() } @to_add);
    if (@to_add) {
        elog(NOTICE, "Merged $tag tag(s) from $source_id to $target_id");
    }
}

my $xml = $target->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) IS 'Given two MARCXML strings and an array of tags, returns MARCXML representing the merge of the specified fields from the second MARCXML record into the first.';

CREATE OR REPLACE FUNCTION migration_tools.make_stub_bib (text[], text[]) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use Text::CSV;

my $in_tags = shift;
my $in_values = shift;

# hack-and-slash parsing of array-passed-as-string;
# this can go away once everybody is running Postgres 9.1+
my $csv = Text::CSV->new({binary => 1});
$in_tags =~ s/^{//;
$in_tags =~ s/}$//;
my $status = $csv->parse($in_tags);
my $tags = [ $csv->fields() ];
$in_values =~ s/^{//;
$in_values =~ s/}$//;
$status = $csv->parse($in_values);
my $values = [ $csv->fields() ];

my $marc = MARC::Record->new();

$marc->leader('00000nam a22000007  4500');
$marc->append_fields(MARC::Field->new('008', '000000s                       000   eng d'));

foreach my $i (0..$#$tags) {
    my ($tag, $sf);
    if ($tags->[$i] =~ /^(\d{3})([0-9a-z])$/) {
        $tag = $1;
        $sf = $2;
        $marc->append_fields(MARC::Field->new($tag, ' ', ' ', $sf => $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    } elsif ($tags->[$i] =~ /^(\d{3})$/) {
        $tag = $1;
        $marc->append_fields(MARC::Field->new($tag, $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    }
}

my $xml = $marc->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.make_stub_bib (text[], text[]) IS $$Simple function to create a stub MARCXML bib from a set of columns.
The first argument is an array of tag/subfield specifiers, e.g., ARRAY['001', '245a', '500a'].
The second argument is an array of text containing the values to plug into each field.  
If the value for a given field is NULL or the empty string, it is not inserted.
$$;

CREATE OR REPLACE FUNCTION migration_tools.set_indicator (TEXT, TEXT, INTEGER, CHAR(1)) RETURNS TEXT AS $func$

my ($marcxml, $tag, $pos, $value) = @_;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use strict;

MARC::Charset->assume_unicode(1);

elog(ERROR, 'indicator position must be either 1 or 2') unless $pos =~ /^[12]$/;
elog(ERROR, 'MARC tag must be numeric') unless $tag =~ /^\d{3}$/;
elog(ERROR, 'MARC tag must not be control field') if $tag =~ /^00/;
elog(ERROR, 'Value must be exactly one character') unless $value =~ /^.$/;

my $xml = $marcxml;
eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ($marc->field($tag)) {
        $field->update("ind$pos" => $value);
    }
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
};
return $xml;

$func$ LANGUAGE PLPERLU;

COMMENT ON FUNCTION migration_tools.set_indicator(TEXT, TEXT, INTEGER, CHAR(1)) IS $$Set indicator value of a specified MARC field.
The first argument is a MARCXML string.
The second argument is a MARC tag.
The third argument is the indicator position, either 1 or 2.
The fourth argument is the character to set the indicator value to.
All occurences of the specified field will be changed.
The function returns the revised MARCXML string.$$;
