\timing on

-- very simple rollback ability but has limitations, hope to address more comprehensively on lp1902233

DROP TABLE IF EXISTS acn_rollback_log;
CREATE TABLE acn_rollback_log (
	id SERIAL
    ,group_id INTEGER
    ,record BIGINT
    ,acns BIGINT[]
    ,holds BIGINT[]
);
CREATE INDEX acn_rollback_log_record ON acn_rollback_log (record);

-- needed for inclusive and subset dedupes 
\pset format unaligned
\x off
\t on
\o ~/bib_dedupe_merges.sql
SELECT '\timing on';
SELECT '\set ECHO all';
SELECT 'SELECT merge_group(' || id || '); SELECT * FROM PG_SLEEP(1);' FROM groups WHERE done = FALSE AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','inclusive')) ORDER BY id;
\o
\t off
\pset format aligned

\i ~/bib_dedupe_merges.sql
