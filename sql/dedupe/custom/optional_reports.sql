/*

This includes an alternative set of summary reports and pairs reports that supplement the group reports in file 06_reports.sql.

The pairs reports are primarily useful for seeing the logic of how records are matched against each other on a 1 to 1 basis.  

To use these the line 'DROP TABLE IF EXISTS pairs;' will need to be removed from 03_groups.sql as it is dropped by default 
as part of cleanup during the dedupe process.

*/ 


\timing on

\x off
\pset format unaligned
\f ','

\o ~/tacs_isbn_pairs.csv
SELECT
	'"' || p.records[1]::TEXT || '"' AS "Record A"
	,'"' || p.records[2]::TEXT || '"' AS "Record B"
	,'"' || ba.search_format_str || '"' AS "Search Format"
	,'"' || ba.title || '"' AS "Normalized Title"
	,'"' || ba.o_title || '"' AS "Title A"
	,'"' || bb.o_title || '"' AS "Title B"
	,'"' || ba.author || '"' AS "Normalized Author"
	,'"' || ba.o_author || '"' AS "Author A"
	,'"' || bb.o_author || '"' AS "Author B"
	,'"' || ARRAY_TO_STRING(ba.isbn_values,' | ') || '"' AS "Record A ISBNs"
	,'"' || ARRAY_TO_STRING(bb.isbn_values,' | ') || '"' AS "Record B ISBNs"
    ,'"' || ba.avdisc_flag::TEXT || ' ' || ARRAY_TO_STRING(ba.description,' | ') || '"' AS "Disc Flag A / Description"
    ,'"' || bb.avdisc_flag::TEXt || ' ' || ARRAY_TO_STRING(bb.description,' | ') || '"' AS "Disc Flag B / Description"
FROM
	pairs p
JOIN
	dedupe_batch ba ON ba.record = p.records[1]
JOIN
	dedupe_batch bb ON bb.record = p.records[2]
WHERE
	p.merge_set = 'tacs isbn'
;   
\o

\o ~/tcs_manga_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"' AS "Record A"
    ,'"' || p.records[2]::TEXT || '"' AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.author || '"' AS "Normalized Author"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ba.titlepart || '"' AS "Normalized Titlepart"
    ,'"' || ba.o_titlepart || '"' AS "Titlepart A"
    ,'"' || bb.o_titlepart || '"' AS "Titlepart B"
    ,'"' || ba.titlepartname || '"' AS "Normalized Titlepartname"
    ,'"' || ba.o_titlepartname || '"' AS "Titlepartname A"
    ,'"' || bb.o_titlepartname || '"' AS "Titlepartname B"
    ,'"' || ARRAY_TO_STRING(ba.all_publishers,' | ') || '"' AS "Publishers A"
    ,'"' || ARRAY_TO_STRING(bb.all_publishers,' | ') || '"' AS "Publishers B"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'tcs manga'
;
\o


\o ~/tacs_issn_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"' AS "Record A"
    ,'"' || p.records[2]::TEXT || '"' AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.author || '"' AS "Normalized Author"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ARRAY_TO_STRING(ba.issn_values,' | ') || '"' AS "Record A ISBNs"
    ,'"' || ARRAY_TO_STRING(bb.issn_values,' | ') || '"' AS "Record B ISBNs"
    ,'"' || ARRAY_TO_STRING(ba.all_pubdates,' | ') || '"' AS "Record A Pubdates"
    ,'"' || ARRAY_TO_STRING(bb.all_pubdates,' | ') || '"' AS "Record B Pubdates"
    ,'"' || ba.avdisc_flag::TEXT || ' ' || ARRAY_TO_STRING(ba.description,' | ') || '"' AS "Disc Flag A / Description"
    ,'"' || bb.avdisc_flag::TEXt || ' ' || ARRAY_TO_STRING(bb.description,' | ') || '"' AS "Disc Flag B / Description"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'tacs issn'
;
\o

\o ~/tacs_upc_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"'  AS "Record A"
    ,'"' || p.records[2]::TEXT || '"'  AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.author || '"' AS "Normalized Author"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ARRAY_TO_STRING(ba.upc_values,' | ') || '"' AS "Record A UPCs"
    ,'"' || ARRAY_TO_STRING(bb.upc_values,' | ') || '"' AS "Record B UPCs"
	,'"' || ba.avdisc_flag::TEXT || ' ' || ARRAY_TO_STRING(ba.description,' | ') || '"' AS "Disc Flag A / Description"
	,'"' || bb.avdisc_flag::TEXt || ' ' || ARRAY_TO_STRING(bb.description,' | ') || '"' AS "Disc Flag B / Description"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'tacs upc'  
;   
\o

\o ~/tacs_pubdate_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"' AS "Record A"
    ,'"' || p.records[2]::TEXT || '"' AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.author || '"' AS "Normalized Author"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ba.titlepart || '"' AS "Normalized Titlepart"
    ,'"' || ba.o_titlepart || '"' AS "Titlepart A"
    ,'"' || bb.o_titlepart || '"' AS "Titlepart B"	
    ,'"' || ba.titlepartname || '"' AS "Normalized Titlepartname"
    ,'"' || ba.o_titlepartname || '"' AS "Titlepartname A"
    ,'"' || bb.o_titlepartname || '"' AS "Titlepartname B"
    ,'"' || ARRAY_TO_STRING(ba.all_pubdates,' | ') || '"' AS "Record A Pubdates"
    ,'"' || ARRAY_TO_STRING(bb.all_pubdates,' | ') || '"' AS "Record B Pubdates"
    ,'"' || ba.avdisc_flag::TEXT || ' ' || ARRAY_TO_STRING(ba.description,' | ') || '"' AS "Disc Flag A / Description"
    ,'"' || bb.avdisc_flag::TEXt || ' ' || ARRAY_TO_STRING(bb.description,' | ') || '"' AS "Disc Flag B / Description"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'tacs pubdate'  
;   
\o

\o ~/tcs_av_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"' AS "Record A"
    ,'"' || p.records[2]::TEXT || '"' AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.author || '"' AS "Normalized Author"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ba.titlepart || '"' AS "Normalized Titlepart"
    ,'"' || ba.o_titlepart || '"' AS "Titlepart A"
    ,'"' || bb.o_titlepart || '"' AS "Titlepart B"  
    ,'"' || ba.titlepartname || '"' AS "Normalized Titlepartname"
    ,'"' || ba.o_titlepartname || '"' AS "Titlepartname A"
    ,'"' || bb.o_titlepartname || '"' AS "Titlepartname B"
    ,'"' || ba.avdisc_flag::TEXT || ' ' || ARRAY_TO_STRING(ba.description,' | ') || '"' AS "Disc Flag A / Description"
    ,'"' || bb.avdisc_flag::TEXt || ' ' || ARRAY_TO_STRING(bb.description,' | ') || '"' AS "Disc Flag B / Description"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'tcs av'  
;
\o

\o ~/local_history_pairs.csv
SELECT
    '"' || p.records[1]::TEXT || '"' AS "Record A"
    ,'"' || p.records[2]::TEXT || '"' AS "Record B"
    ,'"' || ba.search_format_str || '"' AS "Search Format"
    ,'"' || ba.title || '"' AS "Normalized Title"
    ,'"' || ba.o_title || '"' AS "Title A"
    ,'"' || bb.o_title || '"' AS "Title B"
    ,'"' || ba.o_author || '"' AS "Author A"
    ,'"' || bb.o_author || '"' AS "Author B"
    ,'"' || ARRAY_TO_STRING(ba.all_pubdates,' | ') || '"' AS "Record A Pubdates"
    ,'"' || ARRAY_TO_STRING(bb.all_pubdates,' | ') || '"' AS "Record B Pubdates"
FROM
    pairs p
JOIN
    dedupe_batch ba ON ba.record = p.records[1]
JOIN
    dedupe_batch bb ON bb.record = p.records[2]
WHERE
    p.merge_set = 'local history'
;
\o

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
SELECT 'Total Pairs', COUNT(*) FROM pairs;
SELECT 'ISBN match set', COUNT(*) FROM pairs WHERE merge_set = 'tacs isbn';
SELECT 'Manga match set', COUNT(*) FROM pairs WHERE merge_set = 'tcs manga';
SELECT 'ISSN match set', COUNT(*) FROM pairs WHERE merge_set = 'tacs issn';
SELECT 'UPC match set', COUNT(*) FROM pairs WHERE merge_set = 'tacs upc';
SELECT 'A/V match set', COUNT(*) FROM pairs WHERE merge_set = 'tcs av';
SELECT 'Pubdate match set', COUNT(*) FROM pairs WHERE merge_set = 'tacs pubdate';
SELECT 'Local History match set', COUNT(*) FROM pairs WHERE merge_set = 'local history';
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
