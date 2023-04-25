\timing on

-- if doing a migration dedupe 'a' is the incumbent records and 'b' are the incoming 
-- stock isbn match set
UPDATE pairs 
SET merge_set = ARRAY_APPEND(merge_set,'tacs isbn')
WHERE 
    search_format_str 
    AND author 
    AND can_have_copies 
    AND NOT avdisc_flag 
    AND isbn_values 
    AND NOT manga 
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
    AND ( dedupe_setting('page variation') IS NULL OR pages ) -- pages will be null if no setting 
    AND ( dedupe_setting('centimeter variation') IS NULL or centimeters )  
;

UPDATE pairs
SET merge_set = ARRAY_APPEND(merge_set,'tacs isbn and pub info')
WHERE
    search_format_str
    AND author
    AND can_have_copies
    AND NOT avdisc_flag
    AND isbn_values
    AND all_pubdates
    AND all_publishers
    AND NOT manga
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
    AND ( dedupe_setting('page variation') IS NULL OR pages ) -- pages will be null if no setting 
    AND ( dedupe_setting('centimeter variation') IS NULL or centimeters )

UPDATE pairs SET merge_set = ARRAY_APPEND(merge_set,'tacs oclc')
WHERE
    search_format_str
    AND author
    AND can_have_copies
    AND NOT avdisc_flag
    AND oclc_values
    AND NOT manga
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
    AND ( dedupe_setting('page variation') IS NULL OR pages ) -- pages will be null if 
no setting 
    AND ( dedupe_setting('centimeter variation') IS NULL or centimeters )
;

-- stock manga match set based on publisher list 
UPDATE pairs
SET merge_set = ARRAY_APPEND(merge_set,'tacs manga')
WHERE
    search_format_str
    AND author
    AND can_have_copies
    AND NOT avdisc_flag
    AND isbn_values
    AND manga
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
    AND ( dedupe_setting('page variation') IS NULL OR pages ) -- pages will be null if no setting 
    AND ( dedupe_setting('centimeter variation') IS NULL OR centimeters ) -- " 
;


-- stock issn match set 
UPDATE pairs
SET merge_set = ARRAY_APPEND(merge_set,'tacs issn')
WHERE
    search_format_str
    AND author
    AND can_have_copies
    AND NOT avdisc_flag
    AND issn_values
    AND NOT manga
    AND ( titlepart OR titlepart IS NULL )
    AND ( titlepartname OR titlepartname IS NULL )
;

