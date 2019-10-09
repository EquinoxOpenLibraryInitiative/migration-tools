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
-- Then, to move into production, do things like:
--  insert into asset.copy select * from asset_copy where id not in (select id from asset_copy_legacy where not x_migrate);

DROP SCHEMA IF EXISTS migration_tools CASCADE;
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
        sql TEXT;
    BEGIN
        EXECUTE 'DROP TABLE IF EXISTS ' || migration_schema || '.config;';
        EXECUTE 'CREATE TABLE ' || migration_schema || '.config ( key TEXT UNIQUE, value TEXT);';
        EXECUTE 'INSERT INTO ' || migration_schema || '.config (key,value) VALUES ( ''production_tables'', ''asset.call_number,asset.call_number_prefix,asset.call_number_suffix,asset.copy_location,asset.copy,asset.copy_alert,asset.stat_cat,asset.stat_cat_entry,asset.stat_cat_entry_copy_map,asset.copy_note,actor.usr,actor.card,actor.usr_address,actor.stat_cat,actor.stat_cat_entry,actor.stat_cat_entry_usr_map,actor.usr_note,actor.usr_standing_penalty,actor.usr_setting,action.circulation,action.hold_request,action.hold_notification,action.hold_request_note,action.hold_transit_copy,action.transit_copy,money.grocery,money.billing,money.cash_payment,money.forgive_payment,acq.provider,acq.provider_address,acq.provider_note,acq.provider_contact,acq.provider_contact_address,acq.fund,acq.fund_allocation,acq.fund_tag,acq.fund_tag_map,acq.funding_source,acq.funding_source_credit,acq.lineitem,acq.purchase_order,acq.po_item,acq.invoice,acq.invoice_item,acq.invoice_entry,acq.lineitem_detail,acq.fund_debit,acq.fund_transfer,acq.po_note,config.circ_matrix_matchpoint,config.circ_matrix_limit_set_map,config.hold_matrix_matchpoint,asset.copy_tag,asset.copy_tag_copy_map,config.copy_tag_type,serial.item,serial.item_note,serial.record_entry,biblio.record_entry'' );';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build (TEXT) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables TEXT[];
    BEGIN
        SELECT migration_tools.production_tables(migration_schema) INTO STRICT production_tables;
        PERFORM migration_tools.build_base_staging_tables(migration_schema,production_tables);
        EXECUTE 'CREATE UNIQUE INDEX ' || migration_schema || '_patron_barcode_key ON ' || migration_schema || '.actor_card ( barcode );';
        EXECUTE 'CREATE UNIQUE INDEX ' || migration_schema || '_patron_usrname_key ON ' || migration_schema || '.actor_usr ( usrname );';
        EXECUTE 'CREATE UNIQUE INDEX ' || migration_schema || '_copy_barcode_key ON ' || migration_schema || '.asset_copy ( barcode );';
        EXECUTE 'CREATE UNIQUE INDEX ' || migration_schema || '_copy_id_key ON ' || migration_schema || '.asset_copy ( id );';
        EXECUTE 'CREATE INDEX ' || migration_schema || '_callnum_record_idx ON ' || migration_schema || '.asset_call_number ( record );';
        EXECUTE 'CREATE INDEX ' || migration_schema || '_callnum_upper_label_id_lib_idx ON ' || migration_schema || '.asset_call_number ( UPPER(label),id,owning_lib );';
        EXECUTE 'CREATE UNIQUE INDEX ' || migration_schema || '_callnum_label_once_per_lib ON ' || migration_schema || '.asset_call_number ( record,owning_lib,label,prefix,suffix );';
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

CREATE OR REPLACE FUNCTION migration_tools.build_base_staging_tables (TEXT,TEXT[]) RETURNS VOID AS $$
    DECLARE
        migration_schema ALIAS FOR $1;
        production_tables ALIAS FOR $2;
    BEGIN
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
        EXECUTE 'CREATE TABLE ' || migration_schema || '.' || base_staging_table || ' ( LIKE ' || production_table || ' INCLUDING DEFAULTS EXCLUDING CONSTRAINTS );';
        FOR columns IN 
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = migration_schema AND table_name = base_staging_table AND is_nullable = 'NO' AND column_default IS NULL
        LOOP
            EXECUTE 'ALTER TABLE ' || columns.table_schema || '.' || columns.table_name || ' ALTER COLUMN ' || columns.column_name || ' DROP NOT NULL;';
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT VOLATILE;

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
        EXECUTE 'CREATE TABLE ' || migration_schema || '.' || base_staging_table || ' ( LIKE ' || production_table || ' INCLUDING DEFAULTS EXCLUDING CONSTRAINTS );';
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
$function$;

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

