\timing on

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_page_range', (dedupe_setting('page variation')::INTEGER));
END $$;

DO $$
BEGIN
    EXECUTE format('SET %I TO %L', 'var.description_cm_range', (dedupe_setting('cm variation')::INTEGER));
END $$;

-- if doing a migration dedupe 'a' is the incumbent records and 'b' are the incoming 
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

INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs forgiving print'
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
            'book' = ANY(a.search_format)
        )
    AND a.manga = FALSE AND b.manga = FALSE
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND ((a.added_entries && b.added_entries) OR (a.added_entries IS NULL AND b.added_entries IS NULL))
    AND ((a.languages = b.languages) OR (a.languages IS NULL AND b.languages IS NULL))
    AND ( 
          get_descr_part(b.description,'color') = 0
            OR
          ( get_descr_part(a.description,'color') = 1 AND get_descr_part(b.description,'color') = 1 )
            OR
          ( get_descr_part(a.description,'color') = 2 AND get_descr_part(b.description,'color') = 2 )
        )
    AND ( 
          ( 
            get_descr_part(a.description,'pages') > ( get_descr_part(b.description,'pages') - current_setting('var.description_page_range')::INTEGER )
              AND
            get_descr_part(a.description,'pages') < ( get_descr_part(b.description,'pages') + current_setting('var.description_page_range')::INTEGER )
          )
        )
    AND (
          (
            get_descr_part(a.description,'centimeters') > ( get_descr_part(b.description,'centimeters') - current_setting('var.description_cm_range')::INTEGER )
              AND
            get_descr_part(a.description,'centimeters') < ( get_descr_part(b.description,'centimeters') + current_setting('var.description_cm_range')::INTEGER )
          )
        )
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND dedupe_setting('merge tacs forgiving print') = 'TRUE'
;

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



