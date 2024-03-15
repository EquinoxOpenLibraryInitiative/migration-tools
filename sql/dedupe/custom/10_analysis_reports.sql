\timing on

\x off
\pset format unaligned
\f ','

/*
joing inteionally require both recrods to be in 
biblio.record_entry; otherwise it indicates a 
record did merge in another set and in migration 
dedupes one will be incumbent (already in bre) 
and the other to be loaded to bre 

in inclusive they should both be in bre already 
*/

\o summary_of_tacs_isbn_single_point_failures.csv 
SELECT COUNT(*), ARRAY_TO_STRING(p.tacs_isbn_failures,'') 
FROM pairs p
JOIN biblio.record_entry brea ON brea.id = p.records[1]
JOIN biblio.record_entry breb ON breb.id = p.records[2]
WHERE p.merge_set IS NULL AND p.tacs_isbn_score = 1 
GROUP BY 2
ORDER BY 2;
\o

\o tacs_isbn_variations_of_author_failures.csv 
SELECT p.id, dba.record AS "Record A", dbb.record AS "Record B", 
	csv_wrap(dba.o_author) AS "Author A", 
	csv_wrap(dbb.o_author) AS "Author B"
FROM pairs p
JOIN dedupe_batch dba ON dba.record = p.records[1]
JOIN dedupe_batch dbb ON dbb.record = p.records[2]
WHERE p.merge_set IS NULL 
	AND p.tacs_isbn_failures = ARRAY['author']
    AND p.records[1] IN (SELECT id FROM biblio.record_entry)
	AND p.records[2] IN (SELECT id FROM biblio.record_entry) 
ORDER BY p.id
;
\o

\o tacs_isbn_variations_of_search_format_failures.csv
SELECT p.id, dba.record AS "Record A", dbb.record AS "Record B", 
    csv_wrap(dba.search_format_str) AS "Record A Search Formats", 
    csv_wrap(dbb.search_format_str) AS "Record B Search Formats"
FROM pairs p
JOIN dedupe_batch dba ON dba.record = p.records[1]
JOIN dedupe_batch dbb ON dbb.record = p.records[2]
WHERE p.merge_set IS NULL
    AND p.tacs_isbn_failures = ARRAY['search_format']
    AND p.records[1] IN (SELECT id FROM biblio.record_entry)
    AND p.records[2] IN (SELECT id FROM biblio.record_entry)
ORDER BY p.id
;
\o

\o tacs_isbn_variations_of_can_have_copies_failures.csv
SELECT p.id, brea.id AS "Record A", breb.id AS "Record B",
    bsa.source AS "Record A Record Source",
    bsb.source AS "Record B Record Source"
FROM pairs p
JOIN biblio.record_entry brea ON brea.id = p.records[1]
LEFT JOIN config.bib_source bsa ON bsa.id = brea.source 
JOIN biblio.record_entry breb ON breb.id = p.records[2]
LEFT JOIN config.bib_source bsb ON bsb.id = breb.source 
WHERE p.merge_set IS NULL
    AND p.tacs_isbn_failures = ARRAY['can_have_copies']
ORDER BY p.id
;
\o

\t off
\pset format aligned
