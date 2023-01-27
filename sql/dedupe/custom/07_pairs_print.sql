\timing on

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_page_range', (dedupe_setting('page variation')::INTEGER));
END $$;

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_cm_range', (dedupe_setting('cm variation')::INTEGER));
END $$;

SELECT 'page range is ', current_setting('var.description_page_range')::INTEGER;
SELECT 'cm range is ', current_setting('var.description_cm_range')::INTEGER;

-- if doing a migration dedupe 'a' is the incumbent records and 'b' are the incoming 


-- stock isbn match set
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs isbn'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( 
          (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE)
            OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND a.isbn_values && b.isbn_values
    AND a.manga = FALSE AND b.manga = FALSE
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs isbn') = 'TRUE'
;

-- stock isbn and publisher match set, more conservative than just isbn
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs isbn and pub info'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND (
          (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE)
            OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS
NULL)))
        )
    AND a.isbn_values && b.isbn_values
    AND ((a.all_pubdates && b.all_pubdates) OR (a.all_pubdates IS NULL AND b.all_pubdates IS NULL))
    AND ((a.all_publishers && b.all_publishers) OR (a.all_publishers IS NULL AND b.all_publisherse IS NULL))
    AND a.manga = FALSE AND b.manga = FALSE
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND (
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs isbn and pubdate') = 'TRUE'
;

-- stock manga match set based on publisher list 
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tcs manga'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND ((a.subtitle = b.subtitle) OR (a.subtitle IS NULL AND b.subtitle IS NULL))
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND a.search_format_str ~* 'book' AND b.search_format_str ~* 'book'
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND a.manga = TRUE AND b.manga = TRUE
    AND (
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tac manga') = 'TRUE'
;

-- stock issn match set 
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs issn'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE) OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND ((a.all_pubdates && b.all_pubdates) OR (a.all_pubdates IS NULL AND b.all_pubdates IS NULL))
    AND a.issn_values && b.issn_values
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs issn') = 'TRUE'
;

-- not recommended for most libraries 
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs pubdate'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.author = b.author
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE) OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ((a.subtitle = b.subtitle) OR (a.subtitle IS NULL AND b.subtitle IS NULL))
    AND a.all_pubdates && b.all_pubdates
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs pubdate') = 'TRUE'
;



