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
        PERFORM migration_tools.exec( $1, 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.call_number_prefix,asset.call_number_suffix,asset.copy_location,asset.copy,asset.copy_alert,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,actor.usr_standing_penalty,actor.usr_setting,action.circulation,action.hold_request,action.hold_notification,action.hold_request_note,action.hold_transit_copy,action.transit_copy,money.grocery,money.billing,money.cash_payment,money.forgive_payment,acq.provider,acq.provider_address,acq.provider_note,acq.provider_contact,acq.provider_contact_address,acq.fund,acq.fund_allocation,acq.fund_tag,acq.fund_tag_map,acq.funding_source,acq.funding_source_credit,acq.lineitem,acq.purchase_order,acq.po_item,acq.invoice,acq.invoice_item,acq.invoice_entry,acq.lineitem_detail,acq.fund_debit,acq.fund_transfer,acq.po_note,config.circ_matrix_matchpoint,config.circ_matrix_limit_set_map,config.hold_matrix_matchpoint,asset.copy_tag,asset.copy_tag_copy_map,config.copy_tag_type,serial.item,serial.item_note,serial.record_entry,biblio.record_entry'' );' );
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

-- creates other child table so you can have more than one child table in a schema from a base table 
CREATE OR REPLACE FUNCTION build_variant_staging_table(text, text, text)
 RETURNS void
 LANGUAGE plpgsql
 STRICT
AS $function$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_table ALIAS FOR $2;
        base_staging_table ALIAS FOR $3;
        columns RECORD;
    BEGIN
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
$function$

CREATE OR REPLACE FUNCTION migration_tools.create_linked_legacy_table_from (TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        parent_table ALIAS FOR $2;
        source_table ALIAS FOR $3;
        columns RECORD;
        create_sql TEXT;
        insert_sql TEXT;
        column_list TEXT := '';
        column_count INTEGER := 0;
    BEGIN
        create_sql := 'CREATE TABLE ' || migration_schema || '.' || parent_table || '_legacy ( ';
        FOR columns IN
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = migration_schema AND table_name = source_table
        LOOP
            column_count := column_count + 1;
            if column_count > 1 then
                create_sql := create_sql || ', ';
                column_list := column_list || ', ';
            end if;
            create_sql := create_sql || columns.column_name || ' ';
            if columns.data_type = 'ARRAY' then
                create_sql := create_sql || 'TEXT[]';
            else
                create_sql := create_sql || columns.data_type;
            end if;
            column_list := column_list || columns.column_name;
        END LOOP;
        create_sql := create_sql || ' ) INHERITS ( ' || migration_schema || '.' || parent_table || ' );';
        --RAISE INFO 'create_sql = %', create_sql;
        EXECUTE create_sql;
        insert_sql := 'INSERT INTO ' || migration_schema || '.' || parent_table || '_legacy (' || column_list || ') SELECT ' || column_list || ' FROM ' || migration_schema || '.' || source_table || ';';
        --RAISE INFO 'insert_sql = %', insert_sql;
        EXECUTE insert_sql;
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
    SELECT migration_tools._handle_shelf($1,$2,$3,$4,TRUE);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION migration_tools._handle_shelf (TEXT,TEXT,TEXT,INTEGER,BOOLEAN) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_name ALIAS FOR $2;
        org_shortname ALIAS FOR $3;
        org_range ALIAS FOR $4;
        make_assertion ALIAS FOR $5;
        proceed BOOLEAN;
        org INTEGER;
        -- if x_org is on the mapping table, it'll take precedence over the passed org_shortname param
        -- though we'll still use the passed org for the full path traversal when needed
        x_org_found BOOLEAN;
        x_org INTEGER;
        org_list INTEGER[];
        o INTEGER;
        row_count NUMERIC;
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

        EXECUTE 'SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            and column_name = ''x_org''
        )' INTO x_org_found USING table_schema, table_name;

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

        IF x_org_found THEN
            RAISE INFO 'Found x_org column';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = x_org'
                || ' AND NOT b.deleted';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset.copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = x_org'
                || ' AND x_shelf IS NULL'
                || ' AND NOT b.deleted';
        ELSE
            RAISE INFO 'Did not find x_org column';
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1'
                || ' AND NOT b.deleted'
            USING org;
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset_copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1'
                || ' AND x_shelf IS NULL'
                || ' AND NOT b.deleted'
            USING org;
        END IF;

        FOREACH o IN ARRAY org_list LOOP
            RAISE INFO 'Considering org %', o;
            EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
                || ' SET x_shelf = b.id FROM asset.copy_location b'
                || ' WHERE BTRIM(UPPER(a.desired_shelf)) = BTRIM(UPPER(b.name))'
                || ' AND b.owning_lib = $1 AND x_shelf IS NULL'
                || ' AND NOT b.deleted'
            USING o;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            RAISE INFO 'Updated % rows', row_count;
        END LOOP;

        IF make_assertion THEN
            EXECUTE 'SELECT migration_tools.assert(
                NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE desired_shelf <> '''' AND x_shelf IS NULL),
                ''Cannot find a desired location'',
                ''Found all desired locations''
            );';
        END IF;

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
            || ' SET x_org = b.id FROM actor.org_unit b'
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
            || ' WHEN BTRIM(desired_not_migrate) = ''Do Not Migrate'' THEN FALSE'
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

-- convenience function for handling desired_not_migrate

CREATE OR REPLACE FUNCTION migration_tools.handle_barred_or_blocked (TEXT,TEXT) RETURNS VOID AS $$
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
            and column_name = ''desired_barred_or_blocked''
        )' INTO proceed USING table_schema, table_name;
        IF NOT proceed THEN
            RAISE EXCEPTION 'Missing column desired_barred_or_blocked'; 
        END IF;

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_barred';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_barred BOOLEAN';

        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' DROP COLUMN IF EXISTS x_blocked';
        EXECUTE 'ALTER TABLE '
            || quote_ident(table_name)
            || ' ADD COLUMN x_blocked BOOLEAN';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_barred = CASE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Barred'' THEN TRUE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Blocked'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Neither'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = '''' THEN FALSE'
            || ' END';

        EXECUTE 'UPDATE ' || quote_ident(table_name) || ' a'
            || ' SET x_blocked = CASE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Blocked'' THEN TRUE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Barred'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = ''Neither'' THEN FALSE'
            || ' WHEN BTRIM(desired_barred_or_blocked) = '''' THEN FALSE'
            || ' END';

        EXECUTE 'SELECT migration_tools.assert(
            NOT EXISTS (SELECT 1 FROM ' || quote_ident(table_name) || ' WHERE x_barred IS NULL or x_blocked IS NULL),
            ''Not all desired_barred_or_blocked values understood'',
            ''All desired_barred_or_blocked values understood''
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
            || ' SET x_profile = b.id FROM permission.grp_tree b'
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
                    AND owner = ANY ($2)
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
                    AND owner = ANY ($2)
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

function$;

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
                || ' SET ' || quote_ident(column_x) || ' = a.id FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.id FROM ' || quote_ident(table_a) || ' a'
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
                || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE BTRIM(a.' || quote_ident(column_a)
                || ') = BTRIM(b.' || quote_ident(column_b) || ')';
        ELSE
            EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
                || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
                || ' WHERE a.' || quote_ident(column_a)
                || ' = b.' || quote_ident(column_b);
        END IF;

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

-- convenience function for linking two tables, but copying column w into column x instead of "id". Unlike handle_link2, this one won't drop the target column, and it also doesn't have a final boolean argument for btrim
-- e.g. select migration_tools.handle_link3(:'migschema','asset_copy','barcode','test_foo','l_barcode','id','x_acp_id');
CREATE OR REPLACE FUNCTION migration_tools.handle_link3 (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b);

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_null_or_empty_string (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND NULLIF(a.' || quote_ident(column_w) || ','''') IS NOT NULL';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_null (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT NULL';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_true (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT TRUE';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_skip_false (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = a.' || quote_ident(column_w) || ' FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND a.' || quote_ident(column_w) || ' IS NOT FALSE';

    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.handle_link3_concat_skip_null (TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) RETURNS VOID AS $$
    DECLARE
        table_schema ALIAS FOR $1;
        table_a ALIAS FOR $2;
        column_a ALIAS FOR $3;
        table_b ALIAS FOR $4;
        column_b ALIAS FOR $5;
        column_w ALIAS FOR $6;
        column_x ALIAS FOR $7;
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

        EXECUTE 'UPDATE ' || quote_ident(table_b) || ' b'
            || ' SET ' || quote_ident(column_x) || ' = CONCAT_WS('' ; '',b.' || quote_ident(column_x) || ',a.' || quote_ident(column_w) || ') FROM ' || quote_ident(table_a) || ' a'
            || ' WHERE a.' || quote_ident(column_a)
            || ' = b.' || quote_ident(column_b)
            || ' AND NULLIF(a.' || quote_ident(column_w) || ','''') IS NOT NULL';

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
                    AND owner = ANY ($2)
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
                    AND owner = ANY ($2)
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
    END LOOP;

    RETURN TRUE;
END
$function$;


-- convenience function for handling item barcode collisions in asset_copy_legacy

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_barcode_collisions(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT barcode FROM asset_copy_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM asset_copy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy SET barcode = migration_schema || '_internal_collision_' || id || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal collisions', internal_collision_count;
    FOR x_barcode IN SELECT a.barcode FROM asset.copy a, asset_copy_legacy b WHERE x_migrate AND a.deleted IS FALSE AND a.barcode = b.barcode
    LOOP
        FOR x_id IN SELECT id FROM asset_copy_legacy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy_legacy SET barcode = migration_schema || '_incumbent_collision_' || id || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_collision_count := incumbent_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent collisions', incumbent_collision_count;
END
$function$ LANGUAGE plpgsql;

-- convenience function for handling patron barcode/usrname collisions in actor_usr_legacy
-- this should be ran prior to populating actor_card

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_barcode_collisions(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_barcode_collision_count NUMERIC := 0;
    incumbent_usrname_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT usrname FROM actor_usr_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_internal_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal usrname/barcode collisions', internal_collision_count;

    FOR x_barcode IN
        SELECT a.barcode FROM actor.card a, actor_usr_legacy b WHERE x_migrate AND a.barcode = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_incumbent_barcode_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_barcode_collision_count := incumbent_barcode_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent barcode collisions', incumbent_barcode_collision_count;

    FOR x_barcode IN
        SELECT a.usrname FROM actor.usr a, actor_usr_legacy b WHERE x_migrate AND a.usrname = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_incumbent_usrname_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_usrname_collision_count := incumbent_usrname_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent usrname collisions (post barcode collision munging)', incumbent_usrname_collision_count;
END
$function$ LANGUAGE plpgsql;

-- alternate version: convenience function for handling item barcode collisions in asset_copy_legacy

CREATE OR REPLACE FUNCTION migration_tools.handle_asset_barcode_collisions2(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT barcode FROM asset_copy_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM asset_copy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy SET barcode = migration_schema || '_internal_collision_' || id || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal collisions', internal_collision_count;
    FOR x_barcode IN SELECT a.barcode FROM asset.copy a, asset_copy_legacy b WHERE x_migrate AND a.deleted IS FALSE AND a.barcode = b.barcode
    LOOP
        FOR x_id IN SELECT id FROM asset_copy_legacy WHERE barcode = x_barcode
        LOOP
            UPDATE asset_copy_legacy SET barcode = migration_schema || '_' || barcode WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_collision_count := incumbent_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent collisions', incumbent_collision_count;
END
$function$ LANGUAGE plpgsql;

-- alternate version: convenience function for handling patron barcode/usrname collisions in actor_usr_legacy
-- this should be ran prior to populating actor_card

CREATE OR REPLACE FUNCTION migration_tools.handle_actor_barcode_collisions2(migration_schema TEXT) RETURNS VOID AS $function$
DECLARE
    x_barcode TEXT;
    x_id BIGINT;
    row_count NUMERIC;
    internal_collision_count NUMERIC := 0;
    incumbent_barcode_collision_count NUMERIC := 0;
    incumbent_usrname_collision_count NUMERIC := 0;
BEGIN
    FOR x_barcode IN SELECT usrname FROM actor_usr_legacy WHERE x_migrate GROUP BY 1 HAVING COUNT(*) > 1
    LOOP
        FOR x_id IN SELECT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_internal_collision_' || id || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            internal_collision_count := internal_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% internal usrname/barcode collisions', internal_collision_count;

    FOR x_barcode IN
        SELECT a.barcode FROM actor.card a, actor_usr_legacy b WHERE x_migrate AND a.barcode = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_barcode_collision_count := incumbent_barcode_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent barcode collisions', incumbent_barcode_collision_count;

    FOR x_barcode IN
        SELECT a.usrname FROM actor.usr a, actor_usr_legacy b WHERE x_migrate AND a.usrname = b.usrname
    LOOP
        FOR x_id IN SELECT DISTINCT id FROM actor_usr_legacy WHERE x_migrate AND usrname = x_barcode
        LOOP
            UPDATE actor_usr_legacy SET usrname = migration_schema || '_' || usrname WHERE id = x_id;
            GET DIAGNOSTICS row_count = ROW_COUNT;
            incumbent_usrname_collision_count := incumbent_usrname_collision_count + row_count;
        END LOOP;
    END LOOP;
    RAISE INFO '% incumbent usrname collisions (post barcode collision munging)', incumbent_usrname_collision_count;
END
$function$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migration_tools.is_circ_rule_safe_to_delete( test_matchpoint INTEGER ) RETURNS BOOLEAN AS $func$
-- WARNING: Use at your own risk
-- FIXME: not considering marc_type, marc_form, marc_bib_level, marc_vr_format, usr_age_lower_bound, usr_age_upper_bound, item_age
DECLARE
    item_object asset.copy%ROWTYPE;
    user_object actor.usr%ROWTYPE;
    test_rule_object config.circ_matrix_matchpoint%ROWTYPE;
    result_rule_object config.circ_matrix_matchpoint%ROWTYPE;
    safe_to_delete BOOLEAN := FALSE;
    m action.found_circ_matrix_matchpoint;
    n action.found_circ_matrix_matchpoint;
    -- ( success BOOL, matchpoint config.circ_matrix_matchpoint, buildrows INT[] )
    result_matchpoint INTEGER;
BEGIN
    SELECT INTO test_rule_object * FROM config.circ_matrix_matchpoint WHERE id = test_matchpoint;
    RAISE INFO 'testing rule: %', test_rule_object;

    INSERT INTO actor.usr (
        profile,
        usrname,
        passwd,
        ident_type,
        first_given_name,
        family_name,
        home_ou,
        juvenile
    ) SELECT
        COALESCE(test_rule_object.grp, 2),
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        MD5(NOW()::TEXT),
        1,
        'Ima',
        'Test',
        COALESCE(test_rule_object.user_home_ou, test_rule_object.org_unit),
        COALESCE(test_rule_object.juvenile_flag, FALSE)
    ;
    
    SELECT INTO user_object * FROM actor.usr WHERE id = currval('actor.usr_id_seq');

    INSERT INTO asset.call_number (
        creator,
        editor,
        record,
        owning_lib,
        label,
        label_class
    ) SELECT
        1,
        1,
        -1,
        COALESCE(test_rule_object.copy_owning_lib,test_rule_object.org_unit),
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        1
    ;

    INSERT INTO asset.copy (
        barcode,
        circ_lib,
        creator,
        call_number,
        editor,
        location,
        loan_duration,
        fine_level,
        ref,
        circ_modifier
    ) SELECT
        'is_circ_rule_safe_to_delete_' || test_matchpoint || '_' || NOW()::text,
        COALESCE(test_rule_object.copy_circ_lib,test_rule_object.org_unit),
        1,
        currval('asset.call_number_id_seq'),
        1,
        COALESCE(test_rule_object.copy_location,1),
        2,
        2,
        COALESCE(test_rule_object.ref_flag,FALSE),
        test_rule_object.circ_modifier
    ;

    SELECT INTO item_object * FROM asset.copy WHERE id = currval('asset.copy_id_seq');

    SELECT INTO m * FROM action.find_circ_matrix_matchpoint(
        test_rule_object.org_unit,
        item_object,
        user_object,
        COALESCE(test_rule_object.is_renewal,FALSE)
    );
    RAISE INFO '   action.find_circ_matrix_matchpoint(%,%,%,%) = (%,%,%)',
        test_rule_object.org_unit,
        item_object.id,
        user_object.id,
        COALESCE(test_rule_object.is_renewal,FALSE),
        m.success,
        m.matchpoint,
        m.buildrows
    ;

    --  disable the rule being tested to see if the outcome changes
    UPDATE config.circ_matrix_matchpoint SET active = FALSE WHERE id = (m.matchpoint).id;

    SELECT INTO n * FROM action.find_circ_matrix_matchpoint(
        test_rule_object.org_unit,
        item_object,
        user_object,
        COALESCE(test_rule_object.is_renewal,FALSE)
    );
    RAISE INFO 'VS action.find_circ_matrix_matchpoint(%,%,%,%) = (%,%,%)',
        test_rule_object.org_unit,
        item_object.id,
        user_object.id,
        COALESCE(test_rule_object.is_renewal,FALSE),
        n.success,
        n.matchpoint,
        n.buildrows
    ;

    -- FIXME: We could dig deeper and see if the referenced config.rule_*
    -- entries are effectively equivalent, but for now, let's assume no
    -- duplicate rules at that level
    IF (
            (m.matchpoint).circulate = (n.matchpoint).circulate
        AND (m.matchpoint).duration_rule = (n.matchpoint).duration_rule
        AND (m.matchpoint).recurring_fine_rule = (n.matchpoint).recurring_fine_rule
        AND (m.matchpoint).max_fine_rule = (n.matchpoint).max_fine_rule
        AND (
                (m.matchpoint).hard_due_date = (n.matchpoint).hard_due_date
                OR (
                        (m.matchpoint).hard_due_date IS NULL
                    AND (n.matchpoint).hard_due_date IS NULL
                )
        )
        AND (
                (m.matchpoint).renewals = (n.matchpoint).renewals
                OR (
                        (m.matchpoint).renewals IS NULL
                    AND (n.matchpoint).renewals IS NULL
                )
        )
        AND (
                (m.matchpoint).grace_period = (n.matchpoint).grace_period
                OR (
                        (m.matchpoint).grace_period IS NULL
                    AND (n.matchpoint).grace_period IS NULL
                )
        )
        AND (
                (m.matchpoint).total_copy_hold_ratio = (n.matchpoint).total_copy_hold_ratio
                OR (
                        (m.matchpoint).total_copy_hold_ratio IS NULL
                    AND (n.matchpoint).total_copy_hold_ratio IS NULL
                )
        )
        AND (
                (m.matchpoint).available_copy_hold_ratio = (n.matchpoint).available_copy_hold_ratio
                OR (
                        (m.matchpoint).available_copy_hold_ratio IS NULL
                    AND (n.matchpoint).available_copy_hold_ratio IS NULL
                )
        )
        AND NOT EXISTS (
            SELECT limit_set, fallthrough
            FROM config.circ_matrix_limit_set_map
            WHERE active and matchpoint = (m.matchpoint).id
            EXCEPT
            SELECT limit_set, fallthrough
            FROM config.circ_matrix_limit_set_map
            WHERE active and matchpoint = (n.matchpoint).id
        )

    ) THEN
        RAISE INFO 'rule has same outcome';
        safe_to_delete := TRUE;
    ELSE
        RAISE INFO 'rule has different outcome';
        safe_to_delete := FALSE;
    END IF;

    RAISE EXCEPTION 'rollback the temporary changes';

EXCEPTION WHEN OTHERS THEN

    RAISE INFO 'inside exception block: %, %', SQLSTATE, SQLERRM;
    RETURN safe_to_delete;

END;
$func$ LANGUAGE plpgsql;

