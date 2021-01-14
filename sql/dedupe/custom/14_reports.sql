\timing on

\x off
\pset format unaligned
\f ','

\o ~/dedupe_groups_all_report.csv
SELECT 
	'"' || g.lead_record::TEXT || '"' AS "Lead Record"
	,'"' || b.o_title || '"' AS "Title"
	,'"' || b.o_author || '"' AS "Author"
	,'"' || ARRAY_TO_STRING(b.all_pubdates,'/') || '"' AS "Pubdates"
	,'"' || ARRAY_TO_STRING(b.isbn_values,'/') || '"' AS "ISBNs"
	,'"' || ARRAY_TO_STRING(b.upc_values,'/') || '"' AS "UPCs"
    ,'"' || ARRAY_TO_STRING(g.merge_sets,'/') || '"' AS "Merge Sets"
	,'"' || ARRAY_TO_STRING(g.records,' | ') || '"' AS "Subordinate Records"
FROM 
	groups g
JOIN
	dedupe_batch b ON b.record = g.lead_record
;
\o

\o ~/dedupe_groups_morethan4_subordinates.csv
SELECT
    '"' || g.lead_record::TEXT || '"' AS "Lead Record"
    ,'"' || b.o_title || '"' AS "Title"
    ,'"' || b.o_author || '"' AS "Author"
    ,'"' || ARRAY_TO_STRING(b.all_pubdates,'/') || '"' AS "Pubdates"
    ,'"' || ARRAY_TO_STRING(b.isbn_values,'/') || '"' AS "ISBNs"
    ,'"' || ARRAY_TO_STRING(b.upc_values,'/') || '"' AS "UPCs"
    ,'"' || ARRAY_TO_STRING(g.merge_sets,'/') || '"' AS "Merge Sets"
    ,'"' || ARRAY_TO_STRING(g.records,' | ') || '"' AS "Subordinate Records"
FROM
    groups g
JOIN
    dedupe_batch b ON b.record = g.lead_record
WHERE 
	ARRAY_LENGTH(records,1) > 4
;
\o

\t on
\o ~/dedupe_summary_information.csv
SELECT 'Count of Records Analyzed', COUNT(*)FROM dedupe_batch;
SELECT 'Lowest Bib ID of Match Set B', get_floor();
SELECT 'Highest Bib ID of Match Set B', get_ceiling();
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
