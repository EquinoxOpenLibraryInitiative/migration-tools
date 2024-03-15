\timing on

CREATE TABLE bre_rollback_log (
     id                 SERIAL
    ,group_id           INTEGER
    ,merge_time         TIMESTAMPTZ DEFAUlT NOW()
    ,record             BIGINT  -- record being merged
    ,merged_to          BIGINT
    ,holds              BIGINT[]
    ,unmerged           BOOLEAN DEFAULT FALSE
);

CREATE TABLE acn_rollback_log (
     id                 SERIAL
    ,merge_id           INTEGER
    ,original_record    BIGINT
    ,acn                BIGINT
    ,holds              BIGINT[]
);
  
CREATE TABLE acp_rollback_log (
     id                 SERIAL
    ,merge_id           INTEGER
    ,acn                BIGINT
    ,acp                BIGINT
);
  
CREATE TABLE monograph_part_rollback_log (
     id                 SERIAL
    ,merge_id           INTEGER
    ,monograph_part     INTEGER
    ,record             INTEGER
);

-- on reflection, this probably isn't needed   
CREATE TABLE copy_part_rollback_log (
     id                  SERIAL
    ,merge_id           INTEGER
    ,target_copy        BIGINT
    ,part               INTEGER
);

\pset format unaligned
\x off
\t on
\o bib_dedupe_merges.sql
SELECT '\timing on';
SELECT '\set ECHO all';
SELECT 'SELECT merge_group(' || id || '); SELECT * FROM PG_SLEEP(1);' FROM groups WHERE done = FALSE AND EXISTS(SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','inclusive')) ORDER BY id;
\o
\t off
\pset format aligned

\i bib_dedupe_merges.sql
