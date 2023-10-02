DROP AGGREGATE anyarray_agg(anyarray);
DROP FUNCTION m_putnam.anyarray_uniq(anyarray);
DROP FUNCTION anyarray_remove(anyarray,anyarray);
DROP FUNCTION anyarray_remove(anyarray,anynonarray);
DROP FUNCTION log_asset_merges(INTEGER,BIGINT,BIGINT);
DROP FUNCTION dedupe_setting_exists(TEXT);
DROP FUNCTION dedupe_setting(TEXT);
DROP FUNCTION get_descr_part(TEXT, TEXT);
DROP FUNCTION test_for_x_merge_to();
DROP FUNCTION get_ceiling();
DROP FUNCTION get_floor();
DROP FUNCTION find_manga_records();
DROP FUNCTION find_locations_by_names(TEXT, TEXT);
DROP FUNCTION get_6xx_scoring_method();
DROP FUNCTION create_acn_no_holdings();
DROP FUNCTION create_bre_no_holdings();
DROP FUNCTION find_lead_record(group_id, TEXT);
DROP FUNCTION assign_attributes(INTEGER, TEXT);
DROP FUNCTION vivisect_record(BIGINT, TEXT, TEXT);
DROP FUNCTION group_pairs(INTEGER);
DROP FUNCTION clean_author(TEXT);
DROP FUNCTION clean_title(TEXT);
DROP FUNCTION vivisect_marc(TEXT,TEXT);
DROP FUNCTION demerge_group(INTEGER);
DROP FUNCTION demerge_record(BIGINT);
DROP FUNCTION merge_next();
DROP FUNCTION merge_group(INTEGER);
DROP FUNCTION migrate_500s(TEXT[], TEXT);
DROP FUNCTION migrate_oclcs(TEXT[], TEXT);
DROP FUNCTION migrate_isbns(TEXT[], TEXT);
DROP FUNCTION migrate_upcs(TEXT[], TEXT);
DROP FUNCTION anyarray_agg_statefunc(anyarray, anyarray);
DROP FUNCTION anyarray_sort(anyarray);
DROP FUNCTION anyarray_uniq(anyarray);
DROP FUNCTION array_remove_clear_empty(anyarray);
DROP FUNCTION insert_single_subfield_tag(TEXT, TEXT, TEXT, TEXT);
DROP FUNCTION remove_binding_statements(TEXT);
DROP FUNCTION get_dedupe_percent();
DROP FUNCTION csv_wrap(TEXT);
DROP FUNCTION anyarray_remove_clear_empty (anyarray);
DROP FUNCTION clean_non_mig_bibs (integer);
DROP FUNCTION clean_title (text,text);
DROP FUNCTION find_cmm(integer[]);
DROP FUNCTION find_lead_record(bigint);
DROP FUNCTION review_group(bigint);
DROP FUNCTION rand_interval();
DROP FUNCTION map_item_report(bigint);
DROP FUNCTION vivisect_record (integer,text,text);
DROP FUNCTION synccircs();
DROP FUNCTION migrate_dedupe_tags(text[],text);
DROP FUNCTION map_item (bigint);
DROP FUNCTION find_cmm (bigint);

