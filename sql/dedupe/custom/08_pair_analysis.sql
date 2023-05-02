ALTER TABLE pairs SET UNLOGGED;

ALTER TABLE pairs 
	 ADD COLUMN tacs_isbn_score INTEGER DEFAULT 0
	,ADD COLUMN tacs_isbn_and_pub_score INTEGER DEFAULT 0
	,ADD COLUMN tacs_issn_score INTEGER DEFAULT 0
	,ADD COLUMN tacs_upc_score INTEGER DEFAULT 0
	,ADD COLUMN tacs_isbn_failures TEXT[]
	,ADD COLUMN tacs_isbn_and_pub_failures TEXT[]
	,ADD COLUMN tacs_issn_failures TEXT[]
	,ADD COLUMN tacs_upc_failures TEXT[]
;

UPDATE pairs SET 
	 tacs_isbn_failures =   ARRAY['search_format','author','can_have_copies','avdisc_flag','isbns','manga','titlepart','titlepartname','pages','cms']
	,tacs_isbn_and_pub_failures = ARRAY['search_format','author','can_have_copies','avdisc_flag','isbns','manga','titlepart','titlepartname','pages','cms','pubdates','publishers']
    ,tacs_issn_failures = ARRAY['search_format','author','can_have_copies','avdisc_flag','issns','manga','titlepart','titlepartname']
	,tacs_upc_failures = ARRAY['search_format','subtitle','author','can_have_copies','upcs','manga','titlepart','titlepartname','pubdates','disc','minutes']
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'search_format')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'search_format')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'search_format')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'search_format')
WHERE 
	search_format_str
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'author')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'author')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'author')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'author')
WHERE 
	author
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'can_have_copies')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'can_have_copies')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'can_have_copies')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'can_have_copies')
WHERE 
	can_have_copies
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'avdisc_flag')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'avdisc_flag')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'avdisc_flag')
WHERE 
	NOT avdisc_flag 
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'manga')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'manga')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'manga')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'manga')
WHERE 
	NOT manga 
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'titlepart')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'titlepart')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'titlepart')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'titlepart')
WHERE 
	titlepart OR titlepart IS NULL
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'titlepartname')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'titlepartname')
	,tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'titlepartname')
	,tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'titlepartname')
WHERE 
	titlepartname OR titlepartname IS NULL
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'pages')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'pages')
WHERE 
	dedupe_setting('page variation') IS NULL OR pages
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'cms')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'cms')
WHERE 
	dedupe_setting('centimeter variation') IS NULL or centimeters
;

UPDATE pairs SET 
	tacs_isbn_failures = ARRAY_REMOVE(tacs_isbn_failures,'isbns')
	,tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'isbns')
WHERE 
	isbn_values
;

UPDATE pairs SET 
	tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'pubdates')
WHERE 
	all_pubdates
;

UPDATE pairs SET 
	tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'pubdates')
WHERE 
	all_pubdates OR all_pubdates IS NULL
;

UPDATE pairs SET 
	tacs_isbn_and_pub_failures = ARRAY_REMOVE(tacs_isbn_and_pub_failures,'publishers')
WHERE 
	all_publishers
;

UPDATE pairs SET 
	tacs_issn_failures = ARRAY_REMOVE(tacs_issn_failures,'issns')
WHERE 
	issn_values
;

UPDATE pairs SET 
	tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'subtitle')
WHERE 
	subtitle OR subtitle IS NULL
;

UPDATE pairs SET 
	tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'upcs')
WHERE 
	upc_values
;

UPDATE pairs SET 
	tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'discs')
WHERE 
	dedupe_setting('disc variation') IS NULL OR discs
;

UPDATE pairs SET 
	tacs_upc_failures = ARRAY_REMOVE(tacs_upc_failures,'minutes')
WHERE 
	dedupe_setting('minutes variation') IS NULL OR minutes
;

	
UPDATE pairs SET 
	 tacs_isbn_score = ARRAY_LENGTH(tacs_isbn_failures,1)
	,tacs_isbn_and_pub_score = ARRAY_LENGTH(tacs_isbn_and_pub_failures,1)
	,tacs_issn_score = ARRAY_LENGTH(tacs_issn_failures,1) 
	,tacs_upc_score = ARRAY_LENGTH(tacs_upc_failures,1)
;

ALTER TABLE pairs SET LOGGED;
