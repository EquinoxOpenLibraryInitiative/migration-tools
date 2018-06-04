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
-- create some incoming ILS specific staging tables, like CREATE foo.legacy_items ( l_barcode TEXT, .. ) INHERITS (foo.asset_copy);
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
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.call_number_prefix,asset.call_number_suffix,asset.copy_location,asset.copy,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,actor.usr_standing_penalty,actor.usr_setting,action.circulation,action.hold_request,action.hold_notification,action.hold_request_note,action.hold_transit_copy,action.transit_copy,money.grocery,money.billing,money.cash_payment,money.forgive_payment,acq.provider,acq.provider_address,acq.provider_note,acq.provider_contact,acq.provider_contact_address,acq.fund,acq.fund_allocation,acq.fund_tag,acq.fund_tag_map,acq.funding_source,acq.funding_source_credit,acq.lineitem,acq.purchase_order,acq.po_item,acq.invoice,acq.invoice_item,acq.invoice_entry,acq.lineitem_detail,acq.fund_debit,acq.fund_transfer,acq.po_note,config.circ_matrix_matchpoint,config.circ_matrix_limit_set_map,config.hold_matrix_matchpoint,asset.copy_tag,asset.copy_tag_copy_map,config.copy_tag_type,serial.item,serial.item_note,serial.record_entry,biblio.record_entry'' );' );
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
        PERFORM migration_tools.exec( $1, 'CREATE UNIQUE INDEX ' || migration_schema || '_callnum_label_once_per_lib ON ' || migration_schema || '.asset_call_number ( record,owning_lib,label,prefix,suffix );' );
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
        PERFORM migration_tools.exec( $1, 'CREATE UNLOGGED TABLE ' || migration_schema || '.' || base_staging_table || ' ( LIKE ' || production_table || ' INCLUDING DEFAULTS EXCLUDING CONSTRAINTS );' );
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

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_last_first_middle_and_random_affix (TEXT) RETURNS TEXT[] AS $$
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
        IF temp ilike '%JR.%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' );
        END IF;
        IF temp ilike '%SR.%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR\.\\s*', '', 'i' );
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' );
        END IF;
        IF temp like '%III%' THEN
            suffix := 'III';
            temp := REGEXP_REPLACE( temp, E'III', '' );
        END IF;
        IF temp like '%II%' THEN
            suffix := 'II';
            temp := REGEXP_REPLACE( temp, E'II', '' );
        END IF;
        IF temp like '%IV%' THEN
            suffix := 'IV';
            temp := REGEXP_REPLACE( temp, E'IV', '' );
        END IF;

        temp := REGEXP_REPLACE( temp, '\(\)', '');
        family_name := BTRIM( REGEXP_REPLACE(temp,E'^(\\S+).*$',E'\\1') );
        family_name := REGEXP_REPLACE( family_name, ',', '' );
        first_given_name := CASE WHEN temp ~ E'^\\S+$' THEN 'N/A' ELSE BTRIM( REGEXP_REPLACE(temp,E'^\\S+\\s+(\\S+).*$',E'\\1') ) END;
        first_given_name := REGEXP_REPLACE( first_given_name, ',', '' );
        second_given_name := CASE WHEN temp ~ E'^\\S+$' THEN '' ELSE BTRIM( REGEXP_REPLACE(temp,E'^\\S+\\s+\\S+\\s*(.*)$',E'\\1') ) END;
        second_given_name := REGEXP_REPLACE( second_given_name, ',', '' );

        RETURN ARRAY[ family_name, prefix, first_given_name, second_given_name, suffix ];
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION migration_tools.name_parse_out_fuller_last_first_middle_and_random_affix (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        full_name TEXT := $1;
        temp TEXT;
        family_name TEXT := '';
        first_given_name TEXT := '';
        second_given_name TEXT := '';
        suffix TEXT := '';
        prefix TEXT := '';
    BEGIN
        temp := BTRIM(full_name);
        -- Use values, not structure, for prefix/suffix, unless we come up with a better idea
        --IF temp ~ '^\S{2,}\.' THEN
        --    prefix := REGEXP_REPLACE(temp, '^(\S{2,}\.).*$','\1');
        --    temp := BTRIM(REGEXP_REPLACE(temp, '^\S{2,}\.(.*)$','\1'));
        --END IF;
        --IF temp ~ '\S{2,}\.$' THEN
        --    suffix := REGEXP_REPLACE(temp, '^.*(\S{2,}\.)$','\1');
        --    temp := REGEXP_REPLACE(temp, '^(.*)\S{2,}\.$','\1');
        --END IF;
        IF temp ilike '%MR.%' THEN
            prefix := 'Mr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MRS.%' THEN
            prefix := 'Mrs.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MRS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%MS.%' THEN
            prefix := 'Ms.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'MS\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%DR.%' THEN
            prefix := 'Dr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'DR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR.%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%JR,%' THEN
            suffix := 'Jr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'JR,\\s*', ',', 'i' ));
        END IF;
        IF temp ilike '%SR.%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR\.\\s*', '', 'i' ));
        END IF;
        IF temp ilike '%SR,%' THEN
            suffix := 'Sr.';
            temp := BTRIM(REGEXP_REPLACE( temp, E'SR,\\s*', ',', 'i' ));
        END IF;
        IF temp like '%III%' THEN
            suffix := 'III';
            temp := BTRIM(REGEXP_REPLACE( temp, E'III', '' ));
        END IF;
        IF temp like '%II%' THEN
            suffix := 'II';
            temp := BTRIM(REGEXP_REPLACE( temp, E'II', '' ));
        END IF;

        IF temp ~ ',' THEN
            family_name = BTRIM(REGEXP_REPLACE(temp,'^(.*?,).*$','\1'));
            temp := BTRIM(REPLACE( temp, family_name, '' ));
            family_name := REPLACE( family_name, ',', '' );
            IF temp ~ ' ' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(.+)\s(.+)$','\2') );
            ELSE
                first_given_name := temp;
                second_given_name := '';
            END IF;
        ELSE
            IF temp ~ '^\S+\s+\S+\s+\S+$' THEN
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\1') );
                second_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\2') );
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)\s*(\S+)$','\3') );
            ELSE
                first_given_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\1') );
                second_given_name := temp;
                family_name := BTRIM( REGEXP_REPLACE(temp,'^(\S+)\s*(\S+)$','\2') );
            END IF;
        END IF;

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
            IF city_state_zip ~ E'\\s+[A-Z][A-Z]\\s*$' THEN
                state := REGEXP_REPLACE( city_state_zip, E'^.*,?\\s+([A-Z][A-Z])\\s*$', E'\\1' );
                city := REGEXP_REPLACE( city_state_zip, E'^(.*?),?\\s+[A-Z][A-Z](\\s*)$', E'\\1\\2' );
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

-- try to parse data like this: 100 MAIN ST$COVINGTON, GA 30016
CREATE OR REPLACE FUNCTION migration_tools.parse_out_address (TEXT) RETURNS TEXT[] AS $$
    DECLARE
        fullstring TEXT := $1;
        address1 TEXT := '';
        address2 TEXT := '';
        scratch1 TEXT := '';
        scratch2 TEXT := '';
        city TEXT := '';
        state TEXT := '';
        zip TEXT := '';
    BEGIN
        zip := CASE
            WHEN fullstring ~ E'\\d\\d\\d\\d\\d'
            THEN REGEXP_REPLACE( fullstring, E'^.*(\\d\\d\\d\\d\\d-?\\d*).*$', E'\\1' )
            ELSE ''
        END;
        fullstring := REGEXP_REPLACE( fullstring, E'^(.*)\\d\\d\\d\\d\\d-?\\d*(.*)$', E'\\1\\2');

        IF fullstring ~ ',' THEN
            state := REGEXP_REPLACE( fullstring, E'^(.*),(.*)$', E'\\2');
            scratch1 := REGEXP_REPLACE( fullstring, E'^(.*),(.*)$', E'\\1');
        ELSE
            IF fullstring ~ E'\\s+[A-Z][A-Z]\\s*$' THEN
                state := REGEXP_REPLACE( fullstring, E'^.*,?\\s+([A-Z][A-Z])\\s*$', E'\\1' );
                scratch1 := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s+[A-Z][A-Z](\\s*)$', E'\\1\\2' );
            ELSE
                IF fullstring ~ E'^\\S+$'  THEN
                    scratch1 := fullstring;
                    state := 'N/A';
                ELSE
                    state := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\2');
                    scratch1 := REGEXP_REPLACE( fullstring, E'^(.*?),?\\s*(\\S+)\\s*$', E'\\1');
                END IF;
            END IF;
        END IF;

        IF scratch1 ~ '[\$]' THEN
            scratch2 := REGEXP_REPLACE( scratch1, E'^(.+)[\$](.+?)$', E'\\1');
            city := REGEXP_REPLACE( scratch1, E'^(.+)[\$](.+?)$', E'\\2');
        ELSE
            IF scratch1 ~ '\s' THEN
                scratch2 := REGEXP_REPLACE( scratch1, E'^(.+)\\s+(.+?)$', E'\\1');
                city := REGEXP_REPLACE( scratch1, E'^(.+)\\s+(.+?)$', E'\\2');
            ELSE
                scratch2 := 'N/A';
                city := scratch1;
            END IF;
        END IF;

        IF scratch2 ~ '^\d' THEN
            address1 := scratch2;
            address2 := '';
        ELSE
            address1 := REGEXP_REPLACE( scratch2, E'^(.+?)(\\d.+)$', E'\\1');
            address2 := REGEXP_REPLACE( scratch2, E'^(.+?)(\\d.+)$', E'\\2');
        END IF;

        RETURN ARRAY[
             TRIM(BOTH ' ' FROM address1)
            ,TRIM(BOTH ' ' FROM address2)
            ,TRIM(BOTH ' ' FROM city)
            ,TRIM(BOTH ' ' FROM state)
            ,TRIM(BOTH ' ' FROM zip)
        ];
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.parse_out_address2 (TEXT) RETURNS TEXT AS $$
    my ($address) = @_;

    use Geo::StreetAddress::US;

    my $a = Geo::StreetAddress::US->parse_location($address);

    return [
         "$a->{number} $a->{prefix} $a->{street} $a->{type} $a->{suffix}"
        ,"$a->{sec_unit_type} $a->{sec_unit_num}"
        ,$a->{city}
        ,$a->{state}
        ,$a->{zip}
    ];
$$ LANGUAGE PLPERLU STABLE;

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

CREATE OR REPLACE FUNCTION migration_tools.attempt_sierra_timestamp (TEXT,TEXT) RETURNS TIMESTAMP AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output TIMESTAMP;
    BEGIN
            output := REGEXP_REPLACE(attempt_value,E'^(..)(..)(..)(..)(..)$',E'20\\1-\\2-\\3 \\4:\\5')::TIMESTAMP;
            RETURN output;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::TIMESTAMP AS a;'
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

-- add_code39mod43_checkdigit
--   $barcode      source barcode
--
-- If the source string is 13 or 14 characters long and contains only valid
-- Code 39 mod 43 characters, adds or replaces the 14th
-- character with a checkdigit computed according to the usual algorithm for library barcodes
-- using the Code 39 mod 43 symbology - see <http://en.wikipedia.org/wiki/Code_39#Code_39_mod_43>.  If the
-- input string does not meet those requirements, it is returned unchanged.
--
CREATE OR REPLACE FUNCTION migration_tools.add_code39mod43_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    return $barcode if $barcode !~ /^[0-9A-Z. $\/+%-]{13,14}$/;
    $barcode = substr($barcode, 0, 13); # ignore 14th character

    my @valid_chars = split //, '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-. $/+%';
    my %nums = map { $valid_chars[$_] => $_ } (0..42);

    my $total = 0;
    $total += $nums{$_} foreach split(//, $barcode);
    my $remainder = $total % 43;
    my $checkdigit = $valid_chars[$remainder];
    return $barcode . $checkdigit;
$$ LANGUAGE PLPERLU STRICT STABLE;

-- add_mod16_checkdigit
--   $barcode      source barcode
--
-- https://www.activebarcode.com/codes/checkdigit/modulo16.html

CREATE OR REPLACE FUNCTION migration_tools.add_mod16_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    my @digits = split //, $barcode;
    my $total = 0;
    foreach $digit (@digits) {
        if ($digit =~ /[0-9]/) { $total += $digit;
        } elsif ($digit eq '-') { $total += 10;
        } elsif ($digit eq '$') { $total += 11;
        } elsif ($digit eq ':') { $total += 12;
        } elsif ($digit eq '/') { $total += 13;
        } elsif ($digit eq '.') { $total += 14;
        } elsif ($digit eq '+') { $total += 15;
        } elsif ($digit eq 'A') { $total += 16;
        } elsif ($digit eq 'B') { $total += 17;
        } elsif ($digit eq 'C') { $total += 18;
        } elsif ($digit eq 'D') { $total += 19;
        } else { die "invalid digit <$digit>";
        }
    }
    my $remainder = $total % 16;
    my $difference = 16 - $remainder;
    my $checkdigit;
    if ($difference < 10) { $checkdigit = $difference;
    } elsif ($difference == 10) { $checkdigit = '-';
    } elsif ($difference == 11) { $checkdigit = '$';
    } elsif ($difference == 12) { $checkdigit = ':';
    } elsif ($difference == 13) { $checkdigit = '/';
    } elsif ($difference == 14) { $checkdigit = '.';
    } elsif ($difference == 15) { $checkdigit = '+';
    } elsif ($difference == 16) { $checkdigit = 'A';
    } elsif ($difference == 17) { $checkdigit = 'B';
    } elsif ($difference == 18) { $checkdigit = 'C';
    } elsif ($difference == 19) { $checkdigit = 'D';
    } else { die "error calculating checkdigit";
    }

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

CREATE OR REPLACE FUNCTION migration_tools.apply_circ_matrix_to_specific_circ( tablename TEXT, circ BIGINT ) RETURNS VOID AS $$

-- Usage:
--
--   First make sure the circ matrix is loaded and the circulations
--   have been staged to the extent possible (but at the very least
--   circ_lib, target_copy, usr, and *_renewal).  User profiles and
--   circ modifiers must also be in place.
--
--   SELECT migration_tools.apply_circ_matrix_to_specific_circ('m_nwrl.action_circulation', 18391960);
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
  n                    INT := 0;
  n_circs              INT := 1;
  
BEGIN

  --EXECUTE 'SELECT COUNT(*) FROM ' || tablename || ';' INTO n_circs;

  --FOR circ IN EXECUTE ('SELECT id FROM ' || tablename) LOOP

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

  --END LOOP;

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


CREATE OR REPLACE FUNCTION migration_tools.insert_856_9_conditional (TEXT, TEXT) RETURNS TEXT AS $$

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

CREATE OR REPLACE FUNCTION migration_tools.insert_856_9 (TEXT, TEXT) RETURNS TEXT AS $$

  ## USAGE: UPDATE biblio.record_entry SET marc = migration_tools.insert_856_9(marc, 'ABC') WHERE [...];

  my ($marcxml, $shortname) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ( $marc->field('856') ) {
      if ( ! $field->as_string('9') ) {
        $field->add_subfields( '9' => $shortname );
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

eval {
    my $r = MARC::Record->new_from_xml( $xml );
    my $output_xml = $r->as_xml_record();
};
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

CREATE OR REPLACE FUNCTION migration_tools.create_staff_user(
    username TEXT,
    password TEXT,
    org TEXT,
    perm_group TEXT,
    first_name TEXT DEFAULT '',
    last_name TEXT DEFAULT ''
) RETURNS VOID AS $func$
BEGIN
    RAISE NOTICE '%', org ;
    INSERT INTO actor.usr (usrname, passwd, ident_type, first_given_name, family_name, home_ou, profile)
    SELECT username, password, 1, first_name, last_name, aou.id, pgt.id
    FROM   actor.org_unit aou, permission.grp_tree pgt
    WHERE  aou.shortname = org
    AND    pgt.name = perm_group;
END
$func$
LANGUAGE PLPGSQL;

-- example: SELECT * FROM migration_tools.duplicate_template(5,'{3,4}');
CREATE OR REPLACE FUNCTION migration_tools.duplicate_template (INTEGER, INTEGER[]) RETURNS VOID AS $$
    DECLARE
        target_event_def ALIAS FOR $1;
        orgs ALIAS FOR $2;
    BEGIN
        DROP TABLE IF EXISTS new_atevdefs;
        CREATE TEMP TABLE new_atevdefs (atevdef INTEGER);
        FOR i IN array_lower(orgs,1) .. array_upper(orgs,1) LOOP
            INSERT INTO action_trigger.event_definition (
                active
                ,owner
                ,name
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            ) SELECT
                'f'
                ,orgs[i]
                ,name || ' (clone of '||target_event_def||')'
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            FROM
                action_trigger.event_definition
            WHERE
                id = target_event_def
            ;
            RAISE INFO 'created atevdef with id = %', currval('action_trigger.event_definition_id_seq');
            INSERT INTO new_atevdefs SELECT currval('action_trigger.event_definition_id_seq');
            INSERT INTO action_trigger.environment (
                event_def
                ,path
                ,collector
                ,label
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,path
                ,collector
                ,label
            FROM
                action_trigger.environment
            WHERE
                event_def = target_event_def
            ;
            INSERT INTO action_trigger.event_params (
                event_def
                ,param
                ,value
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,param
                ,value
            FROM
                action_trigger.event_params
            WHERE
                event_def = target_event_def
            ;
        END LOOP;
        RAISE INFO '-- UPDATE action_trigger.event_definition SET active = CASE WHEN id = % THEN FALSE ELSE TRUE END WHERE id in (%,%);', target_event_def, target_event_def, (SELECT array_to_string(array_agg(atevdef),',') from new_atevdefs);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- example: SELECT * FROM migration_tools.duplicate_template_but_change_delay(5,'{3,4}','00:30:00'::INTERVAL);
CREATE OR REPLACE FUNCTION migration_tools.duplicate_template_but_change_delay (INTEGER, INTEGER[], INTERVAL) RETURNS VOID AS $$
    DECLARE
        target_event_def ALIAS FOR $1;
        orgs ALIAS FOR $2;
        new_interval ALIAS FOR $3;
    BEGIN
        DROP TABLE IF EXISTS new_atevdefs;
        CREATE TEMP TABLE new_atevdefs (atevdef INTEGER);
        FOR i IN array_lower(orgs,1) .. array_upper(orgs,1) LOOP
            INSERT INTO action_trigger.event_definition (
                active
                ,owner
                ,name
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,delay
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            ) SELECT
                'f'
                ,orgs[i]
                ,name || ' (clone of '||target_event_def||')'
                ,hook
                ,validator
                ,reactor
                ,cleanup_success
                ,cleanup_failure
                ,new_interval
                ,max_delay
                ,usr_field
                ,opt_in_setting
                ,delay_field
                ,group_field
                ,template
                ,granularity
                ,repeat_delay
            FROM
                action_trigger.event_definition
            WHERE
                id = target_event_def
            ;
            RAISE INFO 'created atevdef with id = %', currval('action_trigger.event_definition_id_seq');
            INSERT INTO new_atevdefs SELECT currval('action_trigger.event_definition_id_seq');
            INSERT INTO action_trigger.environment (
                event_def
                ,path
                ,collector
                ,label
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,path
                ,collector
                ,label
            FROM
                action_trigger.environment
            WHERE
                event_def = target_event_def
            ;
            INSERT INTO action_trigger.event_params (
                event_def
                ,param
                ,value
            ) SELECT
                currval('action_trigger.event_definition_id_seq')
                ,param
                ,value
            FROM
                action_trigger.event_params
            WHERE
                event_def = target_event_def
            ;
        END LOOP;
        RAISE INFO '-- UPDATE action_trigger.event_definition SET active = CASE WHEN id = % THEN FALSE ELSE TRUE END WHERE id in (%,%);', target_event_def, target_event_def, (SELECT array_to_string(array_agg(atevdef),',') from new_atevdefs);
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_leader (TEXT) RETURNS TEXT AS $$
    my ($marcxml) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->leader();
    };
    return $field;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tag (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->field($tag);
    };
    return $field->as_string($subfield,$delimiter);
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tags (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT[] AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($tag);
    };
    my @texts;
    foreach my $field (@fields) {
        push @texts, $field->as_string($subfield,$delimiter);
    }
    return \@texts;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.find_hold_matrix_matchpoint (INTEGER) RETURNS INTEGER AS $$
    SELECT action.find_hold_matrix_matchpoint(
        (SELECT pickup_lib FROM action.hold_request WHERE id = $1),
        (SELECT request_lib FROM action.hold_request WHERE id = $1),
        (SELECT current_copy FROM action.hold_request WHERE id = $1),
        (SELECT usr FROM action.hold_request WHERE id = $1),
        (SELECT requestor FROM action.hold_request WHERE id = $1)
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.find_hold_matrix_matchpoint2 (INTEGER) RETURNS SETOF action.matrix_test_result AS $$
    SELECT action.hold_request_permit_test(
        (SELECT pickup_lib FROM action.hold_request WHERE id = $1),
        (SELECT request_lib FROM action.hold_request WHERE id = $1),
        (SELECT current_copy FROM action.hold_request WHERE id = $1),
        (SELECT usr FROM action.hold_request WHERE id = $1),
        (SELECT requestor FROM action.hold_request WHERE id = $1)
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.find_circ_matrix_matchpoint (INTEGER) RETURNS SETOF action.found_circ_matrix_matchpoint AS $$
    SELECT action.find_circ_matrix_matchpoint(
        (SELECT circ_lib FROM action.circulation WHERE id = $1),
        (SELECT target_copy FROM action.circulation WHERE id = $1),
        (SELECT usr FROM action.circulation WHERE id = $1),
        (SELECT COALESCE(
                NULLIF(phone_renewal,false),
                NULLIF(desk_renewal,false),
                NULLIF(opac_renewal,false),
                false
            ) FROM action.circulation WHERE id = $1
        )
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN) RETURNS VOID AS $$
    DECLARE
        test ALIAS FOR $1;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION 'assertion';
        END IF;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN,TEXT) RETURNS VOID AS $$
    DECLARE
        test ALIAS FOR $1;
        msg ALIAS FOR $2;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION '%', msg;
        END IF;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.assert (BOOLEAN,TEXT,TEXT) RETURNS TEXT AS $$
    DECLARE
        test ALIAS FOR $1;
        fail_msg ALIAS FOR $2;
        success_msg ALIAS FOR $3;
    BEGIN
        IF NOT test THEN
            RAISE EXCEPTION '%', fail_msg;
        END IF;
        RETURN success_msg;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- push bib sequence and return starting value for reserved range
CREATE OR REPLACE FUNCTION migration_tools.push_bib_sequence(INTEGER) RETURNS BIGINT AS $$
    DECLARE
        bib_count ALIAS FOR $1;
        output BIGINT;
    BEGIN
        PERFORM setval('biblio.record_entry_id_seq',(SELECT MAX(id) FROM biblio.record_entry) + bib_count + 2000);
        FOR output IN
            SELECT CEIL(MAX(id)/1000)*1000+1000 FROM biblio.record_entry WHERE id < (SELECT last_value FROM biblio.record_entry_id_seq)
        LOOP
            RETURN output;
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- set a new salted password

CREATE OR REPLACE FUNCTION migration_tools.set_salted_passwd(INTEGER,TEXT) RETURNS BOOLEAN AS $$
    DECLARE
        usr_id              ALIAS FOR $1;
        plain_passwd        ALIAS FOR $2;
        plain_salt          TEXT;
        md5_passwd          TEXT;
    BEGIN

        SELECT actor.create_salt('main') INTO plain_salt;

        SELECT MD5(plain_passwd) INTO md5_passwd;
        
        PERFORM actor.set_passwd(usr_id, 'main', MD5(plain_salt || md5_passwd), plain_salt);

        RETURN TRUE;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;


-- convenience functions for handling copy_location maps

CREATE OR REPLACE FUNCTION migration_tools.handle_shelf (TEXT,TEXT,TEXT,INTEGER) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        org_shortname ALIAS FOR $3;
        org_range ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        o INTEGER;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_shelf''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_shelf'; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_shelf';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_shelf INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_shelf = id FROM asset_copy_location b'
            || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
            || ' AND b.owning_lib = $1'
            || ' AND NOT b.deleted'
        USING org;

        FOREACH o IN ARRAY org_list LOOP
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = id FROM asset.copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1 AND x_shelf IS NULL'
                || ' AND NOT b.deleted'
            USING o;
        END LOOP;

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_shelf <> '''' AND x_shelf IS NULL),
            ''Cannot find a desired location'',
            ''Found all desired locations''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling circmod maps

CREATE OR REPLACE FUNCTION migration_tools.handle_circmod (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_circmod''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_circmod'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_circmod';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_circmod TEXT';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.code))';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.name))'
            || ' AND x_circmod IS NULL';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_circmod = code FROM config.circ_modifier b'
            || ' WHERE BTRIM(UPPER(a.desired_circmod)) = BTRIM(UPPER(b.description))'
            || ' AND x_circmod IS NULL';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_circmod <> '''' AND x_circmod IS NULL),
            ''Cannot find a desired circulation modifier'',
            ''Found all desired circulation modifiers''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling item status maps

CREATE OR REPLACE FUNCTION migration_tools.handle_status (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_status''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_status'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_status';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_status INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_status = id FROM config.copy_status b'
            || ' WHERE BTRIM(UPPER(a.desired_status)) = BTRIM(UPPER(b.name))';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_status <> '''' AND x_status IS NULL),
            ''Cannot find a desired copy status'',
            ''Found all desired copy statuses''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for handling org maps

CREATE OR REPLACE FUNCTION migration_tools.handle_org (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_org''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_org'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_org';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_org INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_org = id FROM actor.org_unit b'
            || ' WHERE BTRIM(a.desired_org) = BTRIM(b.shortname)';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_org <> '''' AND x_org IS NULL),
            ''Cannot find a desired org unit'',
            ''Found all desired org units''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired_not_migrate

CREATE OR REPLACE FUNCTION migration_tools.handle_not_migrate (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_not_migrate''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_not_migrate'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_migrate';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_migrate BOOLEAN';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_migrate = CASE'
            || ' WHEN BTRIM(desired_not_migrate) = ''TRUE'' THEN FALSE'
            || ' WHEN BTRIM(desired_not_migrate) = ''DNM'' THEN FALSE'
            || ' WHEN BTRIM(desired_not_migrate) = ''FALSE'' THEN TRUE'
            || ' WHEN BTRIM(desired_not_migrate) = ''Migrate'' THEN TRUE'
            || ' WHEN BTRIM(desired_not_migrate) = '''' THEN TRUE'
            || ' END';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE x_migrate IS NULL),
            ''Not all desired_not_migrate values understood'',
            ''All desired_not_migrate values understood''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired_profile

CREATE OR REPLACE FUNCTION migration_tools.handle_profile (TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''desired_profile''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_profile'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_profile';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_profile INTEGER';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_profile = id FROM permission.grp_tree b'
            || ' WHERE BTRIM(UPPER(a.desired_profile)) = BTRIM(UPPER(b.name))';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_profile <> '''' AND x_profile IS NULL),
            ''Cannot find a desired profile'',
            ''Found all desired profiles''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired actor stat cats

CREATE OR REPLACE FUNCTION migration_tools.vivicate_actor_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        sc TEXT;
        sce TEXT;
    BEGIN

        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;
        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        -- caller responsible for their own truncates though we try to prevent duplicates
        EXECUTE 'INSERT INTO actor_stat_cat (owner, name)
            SELECT DISTINCT
                 $1
                ,BTRIM('||sc||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM actor.stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM actor_stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
            ORDER BY 2;'
        USING org, org_list;

        EXECUTE 'INSERT INTO actor_stat_cat_entry (stat_cat, owner, value)
            SELECT DISTINCT
                COALESCE(
                    (SELECT id
                        FROM actor.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                   ,(SELECT id
                        FROM actor_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                )
                ,$1
                ,BTRIM('||sce||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                    NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NULLIF(BTRIM('||sce||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM actor.stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM actor.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM actor_stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM actor_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                )
            ORDER BY 1,3;'
        USING org, org_list;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        sc TEXT;
        sce TEXT;
    BEGIN
        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sc' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sc' || field_suffix || ' INTEGER';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sce' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sce' || field_suffix || ' INTEGER';


        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sc' || field_suffix || ' = id
            FROM
                (SELECT id, name, owner FROM actor_stat_cat
                    UNION SELECT id, name, owner FROM actor.stat_cat) u
            WHERE
                    BTRIM(UPPER(u.name)) = BTRIM(UPPER(' || sc || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sce' || field_suffix || ' = id
            FROM
                (SELECT id, stat_cat, owner, value FROM actor_stat_cat_entry
                    UNION SELECT id, stat_cat, owner, value FROM actor.stat_cat_entry) u
            WHERE
                    u.stat_cat = x_sc' || field_suffix || '
                AND BTRIM(UPPER(u.value)) = BTRIM(UPPER(' || sce || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sc' || field_suffix || ' <> '''' AND x_sc' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat'',
            ''Found all desired stat cats''
        );';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sce' || field_suffix || ' <> '''' AND x_sce' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat entry'',
            ''Found all desired stat cat entries''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience functions for adding shelving locations
DROP FUNCTION IF EXISTS migration_tools.find_shelf(INT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.find_shelf(org_id INT, shelf_name TEXT) RETURNS INTEGER AS $$
DECLARE
    return_id   INT;
    d           INT;
    cur_id      INT;
BEGIN
    SELECT INTO d MAX(distance) FROM actor.org_unit_ancestors_distance(org_id);
    WHILE d >= 0
    LOOP
        SELECT INTO cur_id id FROM actor.org_unit_ancestor_at_depth(org_id,d);
        SELECT INTO return_id id FROM asset.copy_location WHERE owning_lib = cur_id AND name ILIKE shelf_name;
        IF return_id IS NOT NULL THEN
                RETURN return_id;
        END IF;
        d := d - 1;
    END LOOP;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

-- may remove later but testing using this with new migration scripts and not loading acls until go live

DROP FUNCTION IF EXISTS migration_tools.find_mig_shelf(INT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.find_mig_shelf(org_id INT, shelf_name TEXT) RETURNS INTEGER AS $$
DECLARE
    return_id   INT;
    d           INT;
    cur_id      INT;
BEGIN
    SELECT INTO d MAX(distance) FROM actor.org_unit_ancestors_distance(org_id);
    WHILE d >= 0
    LOOP
        SELECT INTO cur_id id FROM actor.org_unit_ancestor_at_depth(org_id,d);
        
        SELECT INTO return_id id FROM 
            (SELECT * FROM asset.copy_location UNION ALL SELECT * FROM asset_copy_location) x
            WHERE owning_lib = cur_id AND name ILIKE shelf_name;
        IF return_id IS NOT NULL THEN
                RETURN return_id;
        END IF;
        d := d - 1;
    END LOOP;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

-- alternate adding subfield 9 function in that it adds them to existing tags where the 856$u matches a correct value only
DROP FUNCTION IF EXISTS migration_tools.add_sf9(TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.add_sf9(marc TEXT, partial_u TEXT, new_9 TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $matching_u_text = shift;
my $new_9_to_set = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return;
}

my @uris = $marc_xml->field('856');
return unless @uris;

foreach my $field (@uris) {
    my $sfu = $field->subfield('u');
    my $ind2 = $field->indicator('2');
    if (!defined $ind2) { next; }
    if ($ind2 ne '0') { next; }
    if (!defined $sfu) { next; }
    if ($sfu =~ m/$matching_u_text/) {
        $field->add_subfields( '9' => $new_9_to_set );
        last;
    }
}

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.add_sf9(BIGINT, TEXT, TEXT, REGCLASS);
CREATE OR REPLACE FUNCTION migration_tools.add_sf9(bib_id BIGINT, target_u_text TEXT, sf9_text TEXT, bib_table REGCLASS)
    RETURNS BOOLEAN AS
$BODY$
DECLARE
    source_xml    TEXT;
    new_xml       TEXT;
    r             BOOLEAN;
BEGIN

    EXECUTE 'SELECT marc FROM ' || bib_table || ' WHERE id = ' || bib_id INTO source_xml;

    SELECT add_sf9(source_xml, target_u_text, sf9_text) INTO new_xml;

    r = FALSE;
	new_xml = '$_$' || new_xml || '$_$';

    IF new_xml != source_xml THEN
        EXECUTE 'UPDATE ' || bib_table || ' SET marc = ' || new_xml || ' WHERE id = ' || bib_id;
        r = TRUE;
    END IF;

    RETURN r;

END;
$BODY$ LANGUAGE plpgsql;

-- convenience function for linking to the item staging table

CREATE OR REPLACE FUNCTION migration_tools.handle_item_barcode (TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        foreign_column_name ALIAS FOR $3;
        main_column_name ALIAS FOR $4;
        btrim_desired ALIAS FOR $5;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, foreign_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_name, foreign_column_name; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = ''asset_copy_legacy''
            and column_name = $2
        )' INTO proceed USING table_schema, main_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'No %.asset_copy_legacy with column %', table_schema, main_column_name; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_item';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_item BIGINT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_item = b.id FROM asset_copy_legacy b'
                || ' WHERE BTRIM(a.' || quote_ident(foreign_column_name)
                || ') = BTRIM(b.' || quote_ident(main_column_name) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_item = b.id FROM asset_copy_legacy b'
                || ' WHERE a.' || quote_ident(foreign_column_name)
                || ' = b.' || quote_ident(main_column_name);
        END IF;

        --EXECUTE 'SELECT migration_tools.assert(
        --    NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE ' || quote_ident(foreign_column_name) || ' <> '''' AND x_item IS NULL),
        --    ''Cannot link every barcode'',
        --    ''Every barcode linked''
        --);';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking to the user staging table

CREATE OR REPLACE FUNCTION migration_tools.handle_user_barcode (TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        foreign_column_name ALIAS FOR $3;
        main_column_name ALIAS FOR $4;
        btrim_desired ALIAS FOR $5;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, foreign_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_name, foreign_column_name; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = ''actor_usr_legacy''
            and column_name = $2
        )' INTO proceed USING table_schema, main_column_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'No %.actor_usr_legacy with column %', table_schema, main_column_name; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_user';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_user INTEGER';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_user = b.id FROM actor_usr_legacy b'
                || ' WHERE BTRIM(a.' || quote_ident(foreign_column_name)
                || ') = BTRIM(b.' || quote_ident(main_column_name) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_user = b.id FROM actor_usr_legacy b'
                || ' WHERE a.' || quote_ident(foreign_column_name)
                || ' = b.' || quote_ident(main_column_name);
        END IF;

        --EXECUTE 'SELECT migration_tools.assert(
        --    NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE ' || quote_ident(foreign_column_name) || ' <> '''' AND x_user IS NULL),
        --    ''Cannot link every barcode'',
        --    ''Every barcode linked''
        --);';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables
-- e.g. select migration_tools.handle_link(:'migschema','asset_copy','barcode','test_foo','l_barcode','x_acp_id',false);
CREATE OR REPLACE FUNCTION migration_tools.handle_link (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_x ALIAS FOR $6;
        btrim_desired ALIAS FOR $7;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' DROP COLUMN IF EXISTS ' || quote_ident(column_x);
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' ADD COLUMN ' || quote_ident(column_x) || ' BIGINT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = id FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = id FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE a.' || quote_ident(column_a)
                || ' = b.' || quote_ident(column_b);
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables, but copying column w into column x instead of "id"
-- e.g. select migration_tools.handle_link2(:'migschema','asset_copy','barcode','test_foo','l_barcode','id','x_acp_id',false);
CREATE OR REPLACE FUNCTION migration_tools.handle_link2 (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
        btrim_desired ALIAS FOR $8;
        proceed BOOLEAN;
    BEGIN
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_a, column_a;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_a, column_a; 
        END IF;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_b, column_b;
        IF NOT proceed THEN
            RAISE EXCEPTION '%.% missing column %', table_schema, table_b, column_b; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' DROP COLUMN IF EXISTS ' || quote_ident(column_x);
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_b)
            || ' ADD COLUMN ' || quote_ident(column_x) || ' TEXT';

        IF btrim_desired THEN
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = ' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = ' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE a.' || quote_ident(column_a)
                || ' = b.' || quote_ident(column_b);
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for handling desired asset stat cats

CREATE OR REPLACE FUNCTION migration_tools.vivicate_asset_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        sc TEXT;
        sce TEXT;
    BEGIN

        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;
        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        -- caller responsible for their own truncates though we try to prevent duplicates
        EXECUTE 'INSERT INTO asset_stat_cat (owner, name)
            SELECT DISTINCT
                 $1
                ,BTRIM('||sc||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM asset.stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM asset_stat_cat
                    WHERE owner = ANY ($2)
                    AND name = BTRIM('||sc||')
                )
            ORDER BY 2;'
        USING org, org_list;

        EXECUTE 'INSERT INTO asset_stat_cat_entry (stat_cat, owner, value)
            SELECT DISTINCT
                COALESCE(
                    (SELECT id
                        FROM asset.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                   ,(SELECT id
                        FROM asset_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name))
                )
                ,$1
                ,BTRIM('||sce||')
            FROM 
                ' || quote_ident(table_name) || '
            WHERE
                    NULLIF(BTRIM('||sc||'),'''') IS NOT NULL
                AND NULLIF(BTRIM('||sce||'),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT id
                    FROM asset.stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM asset.stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                )
                AND NOT EXISTS (
                    SELECT id
                    FROM asset_stat_cat_entry
                    WHERE stat_cat = (
                        SELECT id
                        FROM asset_stat_cat
                        WHERE owner = ANY ($2)
                        AND BTRIM('||sc||') = BTRIM(name)
                    ) AND value = BTRIM('||sce||')
                )
            ORDER BY 1,3;'
        USING org, org_list;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_sc_and_sce (TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        field_suffix ALIAS FOR $3; -- for distinguishing between desired_sce1, desired_sce2, etc.
        org_shortname ALIAS FOR $4;
        proceed BOOLEAN;
        org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        sc TEXT;
        sce TEXT;
    BEGIN
        SELECT 'desired_sc' || field_suffix INTO sc;
        SELECT 'desired_sce' || field_suffix INTO sce;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sc;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sc; 
        END IF;
        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = $3
        )' INTO proceed USING table_schema, table_name, sce;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column %', sce; 
        END IF;

        SELECT id INTO org FROM actor.org_unit WHERE shortname = org_shortname;
        IF org IS NULL THEN
            RAISE EXCEPTION 'Cannot find org by shortname';
        END IF;

        SELECT INTO org_list ARRAY_ACCUM(id) FROM actor.org_unit_full_path( org );

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sc' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sc' || field_suffix || ' INTEGER';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_sce' || field_suffix;
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_sce' || field_suffix || ' INTEGER';


        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sc' || field_suffix || ' = id
            FROM
                (SELECT id, name, owner FROM asset_stat_cat
                    UNION SELECT id, name, owner FROM asset.stat_cat) u
            WHERE
                    BTRIM(UPPER(u.name)) = BTRIM(UPPER(' || sc || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'UPDATE ' || quote_ident(table_name) || '
            SET
                x_sce' || field_suffix || ' = id
            FROM
                (SELECT id, stat_cat, owner, value FROM asset_stat_cat_entry
                    UNION SELECT id, stat_cat, owner, value FROM asset.stat_cat_entry) u
            WHERE
                    u.stat_cat = x_sc' || field_suffix || '
                AND BTRIM(UPPER(u.value)) = BTRIM(UPPER(' || sce || '))
                AND u.owner = ANY ($1);'
        USING org_list;

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sc' || field_suffix || ' <> '''' AND x_sc' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat'',
            ''Found all desired stat cats''
        );';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_sce' || field_suffix || ' <> '''' AND x_sce' || field_suffix || ' IS NULL),
            ''Cannot find a desired stat cat entry'',
            ''Found all desired stat cat entries''
        );';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

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
    END LOOP;  

    RETURN TRUE;
END
$function$;

