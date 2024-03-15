DROP TABLE IF EXISTS pairs;
CREATE UNLOGGED TABLE pairs (
    id SERIAL
    ,title TEXT
    ,merge_set TEXT[]
    ,records BIGINT[]
    ,grouped BOOLEAN DEFAULT FALSE
    ,subtitle BOOLEAN DEFAULT NULL 
    ,author BOOLEAN DEFAULT NULL
    ,search_format_str BOOLEAN DEFAULT NULL 
    ,all_pubdates BOOLEAN DEFAULT NULL
    ,all_publishers BOOLEAN DEFAULT NULL
    ,description BOOLEAN DEFAULT FALSE
    ,can_have_copies BOOLEAN DEFAULT NULL
    ,avdisc_flag BOOLEAN DEFAULT NULL
    ,manga BOOLEAN DEFAULT NULL
    ,titlepart BOOLEAN DEFAULT NULL
    ,titlepartname BOOLEAN DEFAULT NULL 
    ,upc_values BOOLEAN DEFAULT FALSE
    ,isbn_values BOOLEAN DEFAULT FALSE
    ,issn_values BOOLEAN DEFAULT FALSE
    ,oclc_values BOOLEAN DEFAULT FALSE
    ,centimeters BOOLEAN DEFAULT NULL 
    ,pages BOOLEAN DEFAULT NULL 
    ,discs BOOLEAN DEFAULT NULL 
    ,minutes BOOLEAN DEFAULT NULL 
    
);

INSERT INTO pairs (records, title)
SELECT ARRAY[a.record,b.record], a.title 
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title
    AND a.record != b.record
    AND ( 
            (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
            (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
;

/*
some booleans need to be treated a little differently
becasue we need to know null state 

when the source is a boolean we have three states:
TRUE = both true; FALSE = both FALSE, NULL = no match (default) 
these include : avdisc_flag, manga, can_have_copies 

when the source is a text fields we have three states 
TRUE = match, FALSE = no match; NULL = both are NULL 
these include : author, search_format_str, subtitle,
    titlepart, titlepartname, all_pubdates, all_publishers 

minutes, discs, centimeters and pages are similar to the 
text fields above but values of 0 are treated as null
and unless the appropriate variance settings are in place 
it will always be null 

for these three state ones the default is NULL 

for others the default is FALSE and TRUE means values match
*/

UPDATE pairs p
SET subtitle = CASE
    WHEN (a.subtitle = b.subtitle) THEN TRUE
    WHEN (a.subtitle IS NULL AND b.subtitle IS NULL) THEN NULL 
    ELSE FALSE 
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p
SET author = CASE
    WHEN (a.author = b.author) THEN TRUE
    WHEN (a.author IS NULL AND b.author IS NULL) THEN NULL
    ELSE FALSE
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p
SET search_format_str = CASE
    WHEN (a.search_format_str = b.search_format_str) THEN TRUE
    WHEN (a.search_format_str IS NULL AND b.search_format_str IS NULL) THEN NULL
    ELSE FALSE
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p 
SET all_pubdates = CASE 
	WHEN a.all_pubdates && b.all_pubdates THEN TRUE 
    WHEN a.all_pubdates IS NULL AND b.all_pubdates IS NULL THEN NULL 
    ELSE FALSE 
	END
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p 
SET all_publishers = CASE 
    WHEN a.all_publishers && b.all_publishers THEN TRUE 
    WHEN a.all_publishers IS NULL AND b.all_publishers IS NULL THEN NULL 
    ELSE FALSE 
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p SET description = TRUE 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND a.description = b.description;

UPDATE pairs p 
SET can_have_copies = CASE 
    WHEN (a.can_have_copies AND b.can_have_copies) THEN TRUE 
    WHEN (NOT a.can_have_copies AND NOT b.can_have_copies) THEN FALSE 
    ELSE NULL
    END
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record; 

UPDATE pairs p
SET avdisc_flag = CASE
    WHEN (a.avdisc_flag AND b.avdisc_flag) THEN TRUE
    WHEN (NOT a.avdisc_flag AND NOT b.avdisc_flag) THEN FALSE
    ELSE NULL
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p
SET manga = CASE
    WHEN (a.manga AND b.manga) THEN TRUE
    WHEN (NOT a.manga AND NOT b.manga) THEN FALSE
    ELSE NULL
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p
SET titlepart = CASE
    WHEN (a.titlepart = b.titlepart) THEN TRUE
    WHEN (a.titlepart IS NULL AND b.titlepart IS NULL) THEN NULL
    ELSE FALSE
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p
SET titlepartname = CASE
    WHEN (a.titlepartname = b.titlepartname) THEN TRUE
    WHEN (a.titlepartname IS NULL AND b.titlepartname IS NULL) THEN NULL
    ELSE FALSE
    END
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record;

UPDATE pairs p SET isbn_values = TRUE 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND a.isbn_values && b.isbn_values;

UPDATE pairs p SET issn_values = TRUE 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND a.issn_values && b.issn_values;

UPDATE pairs p SET upc_values = TRUE 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND a.upc_values && b.upc_values;

UPDATE pairs p SET oclc_values = TRUE 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND a.oclc_values && b.oclc_values;

UPDATE pairs p SET centimeters = CASE 
    WHEN ( 
        SUBSTRING(a.description FROM 85 FOR 8)::INTEGER
        BETWEEN ( SUBSTRING(b.description FROM 85 FOR 8)::INTEGER - dedupe_setting('cm variation')::INTEGER )
        AND ( SUBSTRING(b.description FROM 85 FOR 8)::INTEGER + dedupe_setting('cm variation')::INTEGER )
    ) THEN TRUE 
    ELSE FALSE 
    END 
FROM dedupe_batch a, dedupe_batch b 
WHERE p.records[1] = a.record AND p.records[2] = b.record 
AND LENGTH(a.description) = 92 AND LENGTH(b.description) = 92
AND dedupe_setting('cm variation') IS NOT NULL
AND (    SUBSTRING(a.description FROM 85 FOR 8)::INTEGER > 0
      OR SUBSTRING(b.description FROM 85 FOR 8)::INTEGER > 0
    )
;

UPDATE pairs p SET pages = CASE
    WHEN (
        SUBSTRING(a.description FROM 5 FOR 8)::INTEGER
        BETWEEN ( SUBSTRING(b.description FROM 5 FOR 8)::INTEGER - dedupe_setting('page variation')::INTEGER )
        AND ( SUBSTRING(b.description FROM 5 FOR 8)::INTEGER + dedupe_setting('page variation')::INTEGER )
    ) THEN TRUE 
    ELSE FALSE 
    END 
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record
AND LENGTH(a.description) = 92 AND LENGTH(b.description) = 92
AND dedupe_setting('page variation') IS NOT NULL
AND (    SUBSTRING(a.description FROM 5 FOR 8)::INTEGER > 0
      OR SUBSTRING(b.description FROM 5 FOR 8)::INTEGER > 0
    )
;

UPDATE pairs p SET minutes = CASE
    WHEN (
        SUBSTRING(a.description FROM 25 FOR 8)::INTEGER
        BETWEEN ( SUBSTRING(b.description FROM 25 FOR 8)::INTEGER - dedupe_setting('minute variation')::INTEGER )
        AND ( SUBSTRING(b.description FROM 25 FOR 8)::INTEGER + dedupe_setting('minute variation')::INTEGER )
    ) THEN TRUE 
    ELSE FALSE 
    END 
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record
AND LENGTH(a.description) = 92 AND LENGTH(b.description) = 92
AND dedupe_setting('minute variation') IS NOT NULL
AND (    SUBSTRING(a.description FROM 25 FOR 8)::INTEGER > 0
      OR SUBSTRING(b.description FROM 25 FOR 8)::INTEGER > 0
    )
;

UPDATE pairs p SET discs = CASE
    WHEN (
        SUBSTRING(a.description FROM 35 FOR 8)::INTEGER
        BETWEEN ( SUBSTRING(b.description FROM 35 FOR 8)::INTEGER - dedupe_setting('disc variation')::INTEGER )
        AND ( SUBSTRING(b.description FROM 35 FOR 8)::INTEGER + dedupe_setting('disc variation')::INTEGER )
    ) THEN TRUE 
    ELSE FALSE 
    END 
FROM dedupe_batch a, dedupe_batch b
WHERE p.records[1] = a.record AND p.records[2] = b.record
AND LENGTH(a.description) = 92 AND LENGTH(b.description) = 92
AND dedupe_setting('disc variation') IS NOT NULL
AND (    SUBSTRING(a.description FROM 35 FOR 8)::INTEGER > 0
      OR SUBSTRING(b.description FROM 35 FOR 8)::INTEGER > 0
    )
;

CREATE INDEX dedupe_pairs_id_x ON pairs (id);
CREATE INDEX dedupe_pairs_records_x ON pairs USING GIN (records);

ALTER TABLE pairs SET LOGGED;
