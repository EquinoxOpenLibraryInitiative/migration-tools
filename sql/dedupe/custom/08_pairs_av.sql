
-- stock 024 match set, allows authors to be null but requires a pubdate to match or not exist  
UPDATE pairs
SET merge_set = ARRAY_APPEND(merge_set,'tacs upc')
WHERE
    search_format_str
    AND ( subtitle OR subtitle IS NULL )
    AND author
    AND can_have_copies
    AND upc_values
    AND NOT manga
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
    AND ( all_pubdates OR all_pubdates IS NULL)
    AND ( dedupe_setting('disc variation') IS NULL OR discs ) -- discs will be null if no setting 
    AND ( dedupe_setting('minutes variation') IS NULL OR minutes ) -- "
    AND dedupe_setting('merge tacs upc') = 'TRUE' 
;

