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
    AND (a.author = b.author)
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( (a.avdisc_flag = FALSE AND b.avdisc_flag = FALSE) OR
          (a.avdisc_flag AND b.avdisc_flag AND (a.description = b.description OR (a.description IS NULL AND b.description IS NULL)))
        )
    AND a.upc_values && b.upc_values
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs upc') = 'TRUE'
;

INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tcs av'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND ((a.subtitle = b.subtitle) OR (a.subtitle IS NULL AND b.subtitle IS NULL))
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( (NOT a.avdisc_flag AND NOT b.avdisc_flag) OR
          (a.avdisc_flag AND b.avdisc_flag)
        )
    AND ( ('dvd' = ANY(a.search_format) AND 'dvd' = ANY(b.search_format)) OR ('vhs' = ANY(a.search_format) AND 'vhs' = ANY(b.search_format))
        OR ('cd' = ANY(a.search_format) AND 'cd' = ANY(b.search_format)) OR ('cdaudiobook' = ANY(a.search_format) AND 'cdaudiobook' = ANY(b.search_format))
        OR ('blu-ray' = ANY(a.search_format) AND 'blu-ray' = ANY(b.search_format)) OR ('music' = ANY(a.search_format) AND 'music' = ANY(b.search_format))
        OR ('casmusic' = ANY(a.search_format) AND 'casmusic' = ANY(b.search_format)) OR ('cdmusic' = ANY(a.search_format) AND 'cdmusic' = ANY(b.search_format))
        OR ('phonospoken' = ANY(a.search_format) AND 'phonospoken' = ANY(b.search_format))
        OR ('casuadiobook' = ANY(a.search_format) AND 'casaudiobook' = ANY(b.search_format))
    )
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND ((a.all_pubdates && b.all_pubdates) OR (a.all_pubdates IS NULL AND b.all_pubdates IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge authorless av') = 'TRUE'
;

INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs forgiving av'
    ,ARRAY[a.record,b.record]
    ,a.title
FROM
    dedupe_batch a
JOIN
    dedupe_batch b ON b.title = a.title AND a.search_format_str = b.search_format_str
WHERE
    a.record != b.record
    AND a.can_have_copies
    AND b.can_have_copies
    AND ( ('dvd' = ANY(a.search_format) AND 'dvd' = ANY(b.search_format))
        OR ('cd' = ANY(a.search_format) AND 'cd' = ANY(b.search_format))
        OR ('cdaudiobook' = ANY(a.search_format) AND 'cdaudiobook' = ANY(b.search_format))
        OR ('blu-ray' = ANY(a.search_format) AND 'blu-ray' = ANY(b.search_format))
    )
    AND ((a.added_entries && b.added_entries) OR (a.added_entries IS NULL AND b.added_entries IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND ((a.titlepart = b.titlepart) OR (a.titlepart IS NULL AND b.titlepart IS NULL))
    AND ((a.titlepartname = b.titlepartname) OR (a.titlepartname IS NULL AND b.titlepartname IS NULL))
    AND get_descr_part(a.description,'minutes') = get_descr_part(b.description,'minutes')
    AND get_descr_part(a.description,'discs') = get_descr_part(b.description,'discs')
    AND dedupe_setting('merge tacs forgiving av') = 'TRUE'
;


