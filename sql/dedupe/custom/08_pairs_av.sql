
-- stock 024 match set, allows authors to be null but requires a pubdate to match or not exist  
INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs upc'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND ((a.subtitle = b.subtitle) OR (a.subtitle IS NULL AND b.subtitle IS NULL))
    AND ((a.author = b.author) OR (a.author IS NULL AND b.author IS NULL))
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE) OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND a.upc_values && b.upc_values
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ((a.all_pubdates && b.all_pubdates) OR (a.all_pubdates IS NULL AND b.all_pubdates IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs upc') = 'TRUE'
;

