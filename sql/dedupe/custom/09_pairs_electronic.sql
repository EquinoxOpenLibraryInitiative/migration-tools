INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs forgiving ebooks'
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
            'ebook' = ANY(a.search_format)
        )
    AND a.manga = FALSE AND b.manga = FALSE
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND ((a.added_entries && b.added_entries) OR (a.added_entries IS NULL AND b.added_entries IS NULL))
    AND ((a.languages = b.languages) OR (a.languages IS NULL AND b.languages IS NULL))
    AND dedupe_setting('merge tacs forgiving ebooks') = 'TRUE'
;

INSERT INTO pairs (merge_set,records,match_set)
SELECT
    'tacs forgiving e-av'
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
    AND ( ('evideo' = ANY(a.search_format) AND 'evideo' = ANY(b.search_format))
        OR ('eaudio' = ANY(a.search_format) AND 'eaudio' = ANY(b.search_format))
    )
    AND ((a.added_entries && b.added_entries) OR (a.added_entries IS NULL AND b.added_entries IS NULL))
    AND ( 
          (a.record > b.record AND dedupe_setting('dedupe_type') = 'inclusive')
            OR
          (a.staged = FALSE AND b.staged = TRUE AND EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')))
        )
    AND dedupe_setting('merge tacs forgiving e-av') = 'TRUE'
;

