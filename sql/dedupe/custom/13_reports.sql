\timing on

DROP TABLE IF EXISTS groups_copies;
CREATE TABLE groups_copies AS 
WITH bib_list AS (
  SELECT DISTINCT id, bre_id FROM 
  ( SELECT id, lead_record AS bre_id FROM groups
    UNION ALL 
    SELECT id, UNNEST(records) AS bre_id FROM groups
  ) recs
)
SELECT 
   acn.record
  ,bl.id AS group_id 
  ,acp.id AS acp_id 
  ,COALESCE(acp.circ_modifier,'NONE') AS circ_mod
FROM asset.call_number acn 
JOIN bib_list bl ON bl.bre_id = acn.record 
JOIN asset.copy acp ON acp.call_number = acn.id 
WHERE NOT acp.deleted 
;

DROP TABLE IF EXISTS groups_copies_summary;
CREATE TABLE groups_copies_summary AS 
SELECT group_id, COUNT(acp_id) AS acp_count
  ,ARRAY_AGG(DISTINCT circ_mod) AS circ_mods
FROM groups_copies
GROUP BY 1;

\x off
\pset format unaligned
\f ','

\o dedupe_groups_all_report.csv
SELECT 
    '"' || g.lead_record::TEXT || '"' AS "Lead Record"
    ,'"' || b.o_title || '"' AS "Title"
    ,'"' || b.o_author || '"' AS "Author"
    ,'"' || ARRAY_TO_STRING(b.all_pubdates,'/') || '"' AS "Pubdates"
    ,'"' || ARRAY_TO_STRING(b.isbn_values,'/') || '"' AS "ISBNs"
    ,'"' || ARRAY_TO_STRING(b.upc_values,'/') || '"' AS "UPCs"
    ,'"' || ARRAY_TO_STRING(g.merge_sets,'/') || '"' AS "Merge Sets"
    ,'"' || COALESCE(gcs.acp_count::TEXT,'No Copies') || '"' AS "Copy Counts"
    ,'"' || COALESCE(ARRAY_TO_STRING(gcs.circ_mods,' | '),'No Copies') || '"' AS "Circ Mods"
    ,'"' || ARRAY_TO_STRING(g.records,' | ') || '"' AS "Subordinate Records"
FROM 
    groups g
LEFT JOIN 
  groups_copies_summary gcs ON gcs.group_id = g.id 
JOIN
    dedupe_batch b ON b.record = g.lead_record
;
\o

\o dedupe_groups_morethan4_subordinates.csv
SELECT 
    '"' || g.lead_record::TEXT || '"' AS "Lead Record"
    ,'"' || b.o_title || '"' AS "Title"
    ,'"' || b.o_author || '"' AS "Author"
    ,'"' || ARRAY_TO_STRING(b.all_pubdates,'/') || '"' AS "Pubdates"
    ,'"' || ARRAY_TO_STRING(b.isbn_values,'/') || '"' AS "ISBNs"
    ,'"' || ARRAY_TO_STRING(b.upc_values,'/') || '"' AS "UPCs"
    ,'"' || ARRAY_TO_STRING(g.merge_sets,'/') || '"' AS "Merge Sets"
    ,'"' || COALESCE(gcs.acp_count::TEXT,'No Copies') || '"' AS "Copy Counts"
    ,'"' || COALESCE(ARRAY_TO_STRING(gcs.circ_mods,' | '),'No Copies') || '"' AS "Circ Mods"
    ,'"' || ARRAY_TO_STRING(g.records,' | ') || '"' AS "Subordinate Records"
FROM 
    groups g
LEFT JOIN 
  groups_copies_summary gcs ON gcs.group_id = g.id 
JOIN
    dedupe_batch b ON b.record = g.lead_record
WHERE 
	ARRAY_LENGTH(records,1) > 4
;
\o

\t on
\o dedupe_summary_information.csv
SELECT 'Count of Records Analyzed', COUNT(*)FROM dedupe_batch;
SELECT 'Lowest Bib ID of Match Set B', get_floor();
SELECT 'Highest Bib ID of Match Set B', get_ceiling();
SELECT 'Percetange Dedupped', get_dedupe_percent();
SELECT 'Subordinate Records Merged Into Leads', SUM(ARRAY_LENGTH(records,1)) FROM groups;
SELECT 'Count of Final Merge Groups', COUNT(*) FROM groups;
SELECT 'Count of Groups w/ 1 Subordinate Record', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) = 1;
SELECT 'Count of Groups w/ 2 Subordinate Record', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) = 2;
SELECT 'Count of Groups w/ 3 Subordinate Record', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) = 3;
SELECT 'Count of Groups w/ 4 Subordinate Record', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) = 4;
SELECT 'Count of Groups w/ 5-9 Subordinate Records', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) BETWEEN 5 AND 9;
SELECT 'Count of Groups w/ 10-14 Subordinate Records', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) BETWEEN 10 AND 14;
SELECT 'Count of Groups w/ 15-19 Subordinate Records', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) BETWEEN 15 AND 19;
SELECT 'Count of Groups w/ 20+ Subordinate Records', COUNT(*)  FROM groups WHERE ARRAY_LENGTH(records,1) >= 20;
\o
\t off
\pset format aligned
