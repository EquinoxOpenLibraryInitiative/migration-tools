 FUNCTION IF EXISTS log_asset_merges(INTEGER,BIGINT,BIGINT);
CREATE OR REPLACE FUNCTION log_asset_merges(grp_id INTEGER, r BIGINT, lead_record BIGINT)
   RETURNS BOOLEAN
   LANGUAGE plpgsql
AS $function$
DECLARE
	bre_ahrs        BIGINT[];
    acn_ahrs        BIGINT[];
    m_id            INTEGER;
    acn             BIGINT;
    acp             BIGINT;
BEGIN
    -- log the bib merge itself 
    SELECT ARRAY_AGG(ahr.id) 
        FROM action.hold_request ahr
        WHERE ahr.target = r AND ahr.cancel_time IS NULL AND ahr.capture_time IS NULL 
        AND ahr.fulfillment_time IS NULL AND ahr.hold_type = 'T' INTO bre_ahrs;
    INSERT INTO bre_rollback_log (group_id,record,merged_to,holds) VALUES (grp_id,r,lead_record,bre_ahrs) RETURNING id INTO m_id;

    -- log the acns 
    FOR acn IN SELECT id FROM asset.call_number WHERE NOT deleted AND record = r LOOP
        acn_ahrs := NULL;
        SELECT ARRAY_AGG(id) FROM action.hold_request WHERE hold_type = 'V' AND target = acn 
            AND cancel_time IS NULL AND capture_time IS NULL AND fulfillment_time IS NULL INTO acn_ahrs;
        INSERT INTO acn_rollback_log (merge_id,original_record,acn,holds) VALUES (m_id,r,acn,acn_ahrs);
    END LOOP;

    -- log the copies 
    FOR acp, acn IN SELECT id, call_number FROM asset.copy WHERE NOT deleted AND call_number IN (SELECT id FROM asset.call_number WHERE NOT deleted AND record = r) LOOP
        INSERT INTO acp_rollback_log (merge_id, acn, acp) VALUES (m_id,acn,acp); 
    END LOOP; 

    -- log the monograph parts 
    INSERT INTO monograph_part_rollback_log (merge_id,monograph_part,record) SELECT m_id, id, record FROM biblio.monograph_part WHERE NOT deleted AND record = r;

    -- log the monograph part maps 
    INSERT INTO copy_part_rollback_log (merge_id,target_copy,part) SELECT m_id, target_copy, part FROM asset.copy_part_map WHERE part IN 
        (SELECT id FROM biblio.monograph_part WHERE NOT deleted AND record = r);

    RETURN TRUE;
END;
$function$;

DROP FUNCTION IF EXISTS dedupe_setting_exists(TEXT);
CREATE OR REPLACE FUNCTION dedupe_setting_exists(setting_name TEXT)
   RETURNS INTEGER
   LANGUAGE plpgsql
AS $function$
DECLARE
    setting_exists INTEGER DEFAULT 0;
BEGIN
    SELECT 1 FROM dedupe_features WHERE name = setting_name 
    AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1)
    INTO setting_exists;
    RETURN setting_exists;
END;
$function$;

DROP FUNCTION IF EXISTS dedupe_setting(TEXT);
CREATE OR REPLACE FUNCTION dedupe_setting(setting_name TEXT)
   RETURNS TEXT
   LANGUAGE plpgsql
AS $function$
DECLARE
    setting_value TEXT DEFAULT NULL;
    setting_exists INTEGER DEFAULT 0;
BEGIN
    SELECT dedupe_setting_exists(setting_name) INTO setting_exists;
    IF setting_exists = 1 THEN 
        SELECT value FROM dedupe_features WHERE name = setting_name 
            AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1)
            INTO setting_value;
    ELSE         
        RETURN NULL;
    END IF;
    IF BTRIM(setting_value) = '' THEN
        setting_value = NULL;
    END IF;
    IF setting_name = 'merge_group_limit' AND BTRIM(setting_value) = '' THEN  
        setting_value = '15';
	END IF;
    RETURN setting_value;
END;
$function$;

DROP FUNCTION IF EXISTS get_descr_part(TEXT,TEXT);
CREATE OR REPLACE FUNCTION get_descr_part(descrip_str TEXT, part TEXT)
   RETURNS INTEGER
   LANGUAGE plpgsql
AS $function$
DECLARE 
    part_value       INTEGER DEFAULT 0;
    color_part     TEXT;
BEGIN
    IF part = 'pages' THEN SELECT SUBSTRING(descrip_str FROM 5 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'plates' THEN SELECT SUBSTRING(descrip_str FROM 15 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'minutes' THEN SELECT SUBSTRING(descrip_str FROM 25 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'discs' THEN SELECT SUBSTRING(descrip_str FROM 35 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'tapes' THEN SELECT SUBSTRING(descrip_str FROM 45 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'scores' THEN SELECT SUBSTRING(descrip_str FROM 55 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'volumes' THEN SELECT SUBSTRING(descrip_str FROM 65 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'cassettes' THEN SELECT SUBSTRING(descrip_str FROM 75 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'centimeters' THEN SELECT SUBSTRING(descrip_str FROM 85 FOR 8)::INTEGER INTO part_value; END IF;
    IF part = 'color' THEN SELECT LEFT(descrip_str,2) INTO color_part; END IF;
    IF part = 'color' AND color_part = 'un' THEN SELECT 0 INTO part_value; END IF;
    IF part = 'color' AND color_part = 'bw' THEN SELECT 1 INTO part_value; END IF;
    IF part = 'color' AND color_part = 'cl' THEN SELECT 2 INTO part_value; END IF;
    RETURN part_value;
END;
$function$;

DROP FUNCTION IF EXISTS test_for_x_merge_to();
CREATE OR REPLACE FUNCTION test_for_x_merge_to()
 RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    x    BIGINT;
BEGIN
    SELECT value::BIGINT FROM dedupe_features WHERE name = 'incoming ceiling' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO x;
    IF x IS NULL THEN x := 999999999; END IF;

    RETURN TRUE;
END;
$function$;

DROP FUNCTION IF EXISTS get_ceiling();
CREATE OR REPLACE FUNCTION get_ceiling()
 RETURNS BIGINT
 LANGUAGE plpgsql
AS $function$
DECLARE
    x    BIGINT;
BEGIN
    SELECT value::BIGINT FROM dedupe_features WHERE name = 'incoming ceiling' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO x;
    IF x IS NULL THEN x := 999999999; END IF;

    RETURN x;
END;
$function$;

DROP FUNCTION IF EXISTS get_floor();
CREATE OR REPLACE FUNCTION get_floor()
 RETURNS BIGINT
 LANGUAGE plpgsql
AS $function$
DECLARE
    x    BIGINT;
BEGIN
    SELECT value::BIGINT FROM dedupe_features WHERE name = 'incoming floor' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO x;
    IF x IS NULL THEN x := 1; END IF;

    RETURN x;
END;
$function$;

DROP FUNCTION IF EXISTS find_manga_records();
CREATE OR REPLACE FUNCTION find_manga_records()
 RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    x    TEXT;
BEGIN
    FOR x IN SELECT value FROM dedupe_features WHERE name = 'manga publisher' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) LOOP
        UPDATE dedupe_batch SET manga = TRUE WHERE ARRAY_TO_STRING(all_publishers,'') ~* x;
    END LOOP; 

    RETURN TRUE;
END;
$function$;

DROP FUNCTION IF EXISTS find_locations_by_names(TEXT,TEXT);
CREATE OR REPLACE FUNCTION find_locations_by_names(matches_setting TEXT, not_matches_setting TEXT)
 RETURNS TABLE (acl_id INTEGER)
 LANGUAGE plpgsql
AS $function$
DECLARE
    x         TEXT;
    y         INTEGER;
    keep      INTEGER[];
    toss      INTEGER[];
BEGIN
    FOR x IN SELECT value FROM dedupe_features WHERE name = matches_setting LOOP
        SELECT keep || ARRAY_AGG(id) FROM asset.copy_location WHERE name ~* x INTO keep;
    END LOOP;

    x := NULL;    

    FOR x IN SELECT value FROM dedupe_features WHERE name = not_matches_setting LOOP
        SELECT toss || ARRAY_AGG(id) FROM asset.copy_location WHERE name ~* x INTO toss;
    END LOOP;

    keep := ANYARRAY_UNIQ(keep);
    toss := ANYARRAY_UNIQ(toss);

    FOR y IN SELECT UNNEST(toss) LOOP
        keep := ARRAY_REMOVE(keep,y::INTEGER);    
    END LOOP;

    RETURN QUERY SELECT UNNEST(keep);
END;
$function$;

DROP FUNCTION IF EXISTS get_6xx_scoring_method();
CREATE OR REPLACE FUNCTION get_6xx_scoring_method()
 RETURNS TEXT
 LANGUAGE plpgsql
AS $function$
DECLARE
    r   TEXT;
BEGIN
    SELECT COALESCE(value,'default') FROM dedupe_features WHERE name = '6XX scoring' AND org = (SELECT shortname FROM actor.org_unit WHERE id = 1) INTO r;

    RETURN r;
END;
$function$;


DROP FUNCTION IF EXISTS create_acn_no_holdings();
CREATE OR REPLACE FUNCTION create_acn_no_holdings()
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    x   INTERVAL;
    r   INTEGER;
BEGIN

    DROP TABLE IF EXISTS acn_no_holdings;

    SELECT cf.value FROM dedupe_features cf WHERE cf.name = 'remove childless volumes' AND cf.org = (SELECT aou.shortname FROM actor.org_unit aou WHERE aou.id = 1) INTO x;
    IF x IS NULL THEN x := '300 years'::INTERVAL; END IF;

    DROP TABLE IF EXISTS acn_no_holdings;
    CREATE UNLOGGED TABLE acn_no_holdings AS
    SELECT
        acn.id
    FROM
        biblio.record_entry bre
    LEFT JOIN
        asset.call_number acn ON bre.id = acn.record
    LEFT JOIN
        (SELECT id, call_number FROM asset.copy WHERE deleted = FALSE) ac ON ac.call_number = acn.id
    LEFT JOIN
        config.bib_source source ON source.id = bre.source
    WHERE
        acn.deleted = FALSE
        AND ac.id IS NULL
        AND acn.deleted = FALSE
        AND acn.create_date < NOW() - x
        AND acn.edit_date < NOW() - x
        AND (source.can_have_copies = TRUE OR source.id IS NULL)
        AND acn.label !~* '##URI##'
    GROUP BY 1
    HAVING COUNT(ac.id) = 0
    ;

    SELECT COUNT(*) FROM acn_no_holdings INTO r;

    RETURN r;
END;
$function$;


DROP FUNCTION IF EXISTS create_bre_no_holdings();
CREATE OR REPLACE FUNCTION create_bre_no_holdings()
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    x   INTERVAL;
    r   INTEGER;
BEGIN

    DROP TABLE IF EXISTS bre_no_holdings;

    SELECT cf.value FROM dedupe_features cf WHERE cf.name = 'remove childless bibs' AND cf.org = (SELECT aou.shortname FROM actor.org_unit aou WHERE aou.id = 1) INTO x;
    IF x IS NULL THEN x := '300 years'::INTERVAL; END IF;

    CREATE UNLOGGED TABLE bre_no_holdings AS
    SELECT
        bre.id
    FROM
        biblio.record_entry bre
    LEFT JOIN
        (SELECT x.id, x.record FROM asset.call_number x LEFT JOIN acn_no_holdings y ON y.id = x.id WHERE x.deleted = FALSE AND y.id IS NULL) acn ON acn.record = bre.id
    LEFT JOIN
        (SELECT id, call_number FROM asset.copy WHERE deleted = FALSE) ac ON ac.call_number = acn.id
    LEFT JOIN
        config.bib_source source ON source.id = bre.source
    LEFT JOIN 
        serial.record_entry sre ON sre.record = bre.id
    LEFT JOIN
        booking.resource_type brt ON brt.record = bre.id
    WHERE
        bre.deleted IS FALSE
        AND ac.id IS NULL
        AND acn.id IS NULL
        AND (source.can_have_copies = TRUE OR source.id IS NULL)
        AND bre.create_date < NOW() - x
        AND bre.id != -1
        AND brt.record IS NULL
        AND (sre.record IS NULL OR sre.deleted)
    GROUP BY 1
    HAVING COUNT(ac.id) = 0
    ;

    SELECT COUNT(*) FROM bre_no_holdings INTO r;

    RETURN r;
END;
$function$;

DROP FUNCTION IF EXISTS find_lead_record(BIGINT,TEXT);
CREATE OR REPLACE FUNCTION find_lead_record(group_id BIGINT, dedupe_type TEXT)
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    rec_id      BIGINT;
    rec_score   BIGINT;
    x           BIGINT;
    bibfloor    BIGINT;
    bibceiling  BIGINT;
    keep_us     BIGINT[];
BEGIN
    IF dedupe_type = 'inclusive' THEN 
        SELECT b.record, b.score + b.score_bonus - b.score_penalty 
            FROM (SELECT id, UNNEST(records) AS record FROM groups WHERE id = group_id) q
            JOIN dedupe_batch b ON b.record = q.record
            ORDER BY 2 DESC, b.record DESC
            LIMIT 1 INTO rec_id, rec_score;
    ELSE
        SELECT b.record, b.score + b.score_bonus - b.score_penalty
            FROM (SELECT id, UNNEST(records) AS record FROM groups WHERE id = group_id) q
            JOIN (SELECT * FROM dedupe_batch WHERE staged = FALSE) AS b ON b.record = q.record
            ORDER BY 2 DESC, b.record DESC
            LIMIT 1 INTO rec_id, rec_score;
    END IF;

    SELECT records FROM groups WHERE id = group_id INTO keep_us;
    keep_us := ARRAY_REMOVE(keep_us,x::BIGINT);

    SELECT get_floor() INTO bibfloor;
    SELECT get_ceiling() INTO bibceiling;

    IF dedupe_type != 'inclusive' THEN
        FOR x IN SELECT UNNEST(keep_us) LOOP
            IF x < bibfloor OR x > bibceiling THEN
                keep_us := ARRAY_REMOVE(keep_us,x::BIGINT);
            END IF;
          END LOOP;
    END IF;

    UPDATE groups SET 
        score = rec_score, 
        lead_record = rec_id, 
        lead_selected = TRUE, 
        records = keep_us
        WHERE id = group_id;

    RETURN group_id;
END;
$function$;

DROP FUNCTION IF EXISTS assign_attributes(INTEGER,TEXT);
CREATE OR REPLACE FUNCTION assign_attributes(bib_id INTEGER, source TEXT)
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    ra     INTEGER[];
    sfs    TEXT[];
    cont   TEXT[];
    carr   TEXT[];
    media  TEXT[];
    lang   TEXT[];
BEGIN
    IF source = 'production' THEN
        SELECT q.values 
        FROM (SELECT ARRAY_AGG(raf.value) AS values FROM metabib.record_attr_flat raf WHERE raf.attr = 'search_format' AND raf.id = bib_id AND raf.value IS NOT NULL) q 
        INTO sfs;
        SELECT q.values 
        FROM (SELECT ARRAY_AGG(raf.value) AS values FROM metabib.record_attr_flat raf WHERE raf.attr = 'content_type' AND raf.id = bib_id AND raf.value IS NOT NULL) q 
        INTO cont;
        SELECT q.values
        FROM (SELECT ARRAY_AGG(raf.value) AS values FROM metabib.record_attr_flat raf WHERE raf.attr = 'carrier_type' AND raf.id = bib_id AND raf.value IS NOT NULL) q             
        INTO carr;
        SELECT q.values
        FROM (SELECT ARRAY_AGG(raf.value) AS values FROM metabib.record_attr_flat raf WHERE raf.attr = 'media_type' AND raf.id = bib_id AND raf.value IS NOT NULL) q             
        INTO media;
        SELECT q.values
        FROM (SELECT ARRAY_AGG(raf.value) AS values FROM metabib.record_attr_flat raf WHERE raf.attr = 'item_lang' AND raf.id = bib_id AND raf.value IS NOT NULL) q             
        INTO lang;
    ELSE
        SELECT * FROM migration_tools.reingest_staged_record_attributes(bib_id) INTO ra;
        SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(ra)) AND ctype = 'search_format' INTO sfs;
        SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(ra)) AND ctype = 'content_type' INTO cont;
        SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(ra)) AND ctype = 'carrier_type' INTO carr;
        SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(ra)) AND ctype = 'media_type' INTO media;
        SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(ra)) AND ctype = 'item_lang' INTO lang;
    END IF;
    UPDATE dedupe_batch SET search_format = sfs, content_type = cont, carrier_type = carr, media_type = media, languages = lang, populated = TRUE WHERE record = bib_id;
    RETURN bib_id;
END;
$function$;

DROP FUNCTION IF EXISTS vivisect_record(INTEGER,TEXT,TEXT); -- from older version
DROP FUNCTION IF EXISTS vivisect_record(BIGINT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION vivisect_record(bib_id BIGINT, score_method TEXT, source TEXT)
 RETURNS BIGINT
 LANGUAGE plpgsql
AS $function$
DECLARE
    z   TEXT[];
    ra  INTEGER[];
BEGIN
    IF source = 'production' THEN 
        SELECT vivisect_marc(marc, score_method) FROM biblio.record_entry WHERE id = bib_id INTO z;
    ELSE 
        SELECT vivisect_marc(marc, score_method) FROM m_biblio_record_entry WHERE id = bib_id INTO z;
    END IF;
    UPDATE dedupe_batch SET
        subject_score = z[1]::INTEGER
        ,isbn_score = z[2]::INTEGER
        ,issn_score = z[3]::INTEGER
        ,ost_score = z[4]::INTEGER
        ,linked_entry_score = z[5]::INTEGER
        ,added_entry_score = z[6]::INTEGER
        ,authority_score = z[7]::INTEGER
        ,description_score = z[8]::INTEGER
        ,author_score = z[9]::INTEGER
        ,edition_score = z[10]::INTEGER
        ,title_score = z[11]::INTEGER
        ,heading_score = z[12]::INTEGER
        ,score = z[13]::INTEGER
        ,o_title = z[14]
        ,o_subtitle = z[15]
        ,o_author = z[16]
        ,upc_values = STRING_TO_ARRAY(z[17],',')
        ,isbn_values = STRING_TO_ARRAY(z[18],',')
        ,issn_values = STRING_TO_ARRAY(z[19],',')
        ,pubdate = STRING_TO_ARRAY(z[20],',')
        ,rda_pubdate = STRING_TO_ARRAY(z[21],',')
        ,oclc_values = STRING_TO_ARRAY(z[22],',')
        ,description = z[23]
        ,o_titlepart = z[24]
        ,o_titlepartname = z[25]
        ,publisher = STRING_TO_ARRAY(z[26],',')
        ,rda_publisher = STRING_TO_ARRAY(z[27],',')
        ,edition_terms = STRING_TO_ARRAY(z[28],',')
        ,added_entries = STRING_TO_ARRAY(z[29],',')
    WHERE record = bib_id;
    UPDATE dedupe_batch SET populated = TRUE WHERE record = bib_id;
    RETURN bib_id;
END;
$function$;

DROP FUNCTION IF EXISTS group_pairs(INTEGER);
CREATE OR REPLACE FUNCTION group_pairs(pair_id INTEGER)
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    g_ida         INTEGER;
    g_idb         INTEGER;
    p_records     BIGINT[];
    p_record_a    BIGINT;
    p_record_b    BIGINT;
    p_merge_set   TEXT[];
    p_title       TEXT;
BEGIN
    SELECT records[1], records[2], records, merge_set, title FROM pairs WHERE id = pair_id
        INTO p_record_a, p_record_b, p_records, p_merge_set, p_title;

    SELECT id FROM groups WHERE title = p_title AND p_record_a = ANY(records) INTO g_ida;
    SELECT id FROM groups WHERE title = p_title AND p_record_b = ANY(records) INTO g_idb;

    IF g_ida IS NULL AND g_idb IS NULL THEN
        INSERT INTO groups (records, pairs, merge_sets, title)
            SELECT p_records, ARRAY[pair_id], p_merge_set, p_title;
    END IF;

    IF g_ida IS NOT NULL AND g_idb IS NOT NULL AND g_ida != g_idb THEN
        UPDATE groups a SET records = a.records || b.records, 
            pairs = a.pairs || b.pairs, 
            merge_sets = a.merge_sets || b.merge_sets
            FROM (SELECT * FROM groups WHERE id = g_idb) b WHERE a.id = g_ida;
        DELETE FROM groups WHERE id = g_idb;
        UPDATE groups SET records = ANYARRAY_UNIQ(records), merge_sets = ANYARRAY_UNIQ(merge_sets) WHERE id = g_ida; 
        RETURN pair_id;
    END IF;

    IF g_ida IS NOT NULL AND g_idb IS NULL THEN
        UPDATE groups SET
             records = records || p_records
            ,pairs = ANYARRAY_UNIQ(pairs || pair_id)
            ,merge_sets = ANYARRAY_UNIQ(merge_sets || p_merge_set)
        WHERE id = g_ida;
    END IF;

    IF g_ida IS NULL AND g_idb IS NOT NULL THEN
        UPDATE groups SET
             records = records || p_records
            ,pairs = ANYARRAY_UNIQ(pairs || pair_id)
            ,merge_sets = ANYARRAY_UNIQ(merge_sets || p_merge_set)
        WHERE id = g_idb;
    END IF;

    UPDATE pairs SET grouped = TRUE WHERE id = pair_id;
    RETURN pair_id;
END;
$function$;

DROP FUNCTION IF EXISTS clean_author(TEXT);
CREATE OR REPLACE FUNCTION clean_author(author TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE 
    old   TEXT;
    new   TEXT;
BEGIN
    author := LOWER(author);

    FOR old, new IN SELECT to_replace, replace_with FROM chars_to_normalize LOOP
        author := REPLACE(author,old,new);
    END LOOP;

    author := REGEXP_REPLACE(author,'\[(.*?)\]','');
    author := REGEXP_REPLACE(author,'\((.*?)\)','');
    author := BTRIM(REGEXP_REPLACE(author, '[^a-z]', '', 'g'));

    RETURN author;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS clean_title(TEXT);
CREATE OR REPLACE FUNCTION clean_title(title TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE 
    old        TEXT;
    new        TEXT;        
BEGIN
    title := LOWER(title);

    FOR old, new IN SELECT to_replace, replace_with FROM chars_to_normalize LOOP
        title := REPLACE(title,old,new);
    END LOOP;

    FOR old, new IN SELECT to_replace, replace_with FROM title_strings LOOP
        title := REGEXP_REPLACE(title,old,new);
    END LOOP;

    title := BTRIM(REGEXP_REPLACE(title, '[^a-z0-9]', '', 'g'));

    RETURN title;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS vivisect_marc(TEXT,TEXT);
CREATE OR REPLACE FUNCTION vivisect_marc(TEXT,TEXT)
 RETURNS TEXT[]
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');
use Business::ISBN;

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my ($xml, $score_method) = @_;

$xml =~ s/(<leader>.........)./${1}a/;
my $marc;
eval {
    $marc = MARC::Record->new_from_xml($xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return;
}

my @upcs = $marc->field('024');
my @norm_upcs;
if (scalar @upcs > 0) { @norm_upcs = norm_upcs(@upcs); }
my $concat_upcs = '';
foreach my $upc (@norm_upcs) {
   if (!defined $concat_upcs) { $concat_upcs = $upc; }
    else { $concat_upcs = $concat_upcs . ',' . $upc; }
}

my @isbns = $marc->field('020');
my @norm_isbns;
if (scalar @isbns > 0) { @norm_isbns = norm_isbns(@isbns); }
my $concat_isbns = '';
foreach my $isbn (@norm_isbns) {
   if (!defined $concat_isbns) { $concat_isbns = $isbn; }
    else { $concat_isbns = $concat_isbns . ',' . $isbn; }
}

my @issns = $marc->field('022');
my @norm_issns;
if (scalar @issns > 0) { @norm_issns = norm_issns(@issns); }
my $concat_issns = '';
foreach my $issn (@norm_issns) {
   if (!defined $concat_issns) { $concat_issns = $issn; }
    else { $concat_issns = $concat_issns . ',' . $issn; }
}

my @oclcs = $marc->field('035');
my @norm_oclcs;
if (scalar @oclcs > 0) { @norm_oclcs = norm_oclcs(@oclcs); }
my $concat_oclcs = '';
foreach my $oclc (@norm_oclcs) {
   if (!defined $concat_oclcs) { $concat_oclcs = $oclc; }
    else { $concat_oclcs = $concat_oclcs . ',' . $oclc; }
}

my $main_author = $marc->field('100');
my $main_author_str;
if ($main_author) { $main_author_str = $main_author->as_string('a'); }
my $corp_author = $marc->field('110');
my $corp_author_str;
if ($corp_author) { $corp_author_str = $corp_author->as_string('a'); }
my $meet_author = $marc->field('111');
my $meet_author_str;
if ($meet_author) { $meet_author_str = $meet_author->as_string('a'); }
my $author_str;
if ($main_author_str) { $author_str = $main_author_str; } else 
    {
        if ($corp_author_str) { $author_str = $corp_author_str; } else 
        {
            if ($meet_author_str) { $author_str = $meet_author_str; } else { $author_str = ''; }
        }
    } 

my $title = $marc->field('245');

my $title_str;
my $subtitle_str;
my $titlepart_str;
my $titlepartname_str;
if ($title) { 
    $title_str = $title->as_string('a'); 
    $subtitle_str = $title->as_string('b');
    $titlepart_str = $title->as_string('n');
    $titlepartname_str = $title->as_string('p');
    }
if (!defined $title_str) { $title_str = ''; }
if (!defined $subtitle_str) { $subtitle_str = ''; }
if (!defined $titlepart_str) { $titlepart_str = ''; }

my @edition_fields = $marc->field('250');
my $concat_edition_terms = '';
foreach my $edition_field (@edition_fields) {
    my $efa = $edition_field->subfield('a');
    if (!defined $efa) { $efa = ''; }
    $efa = lc($efa);
    if ($efa =~ m/hardback/ or $efa =~ m/hard back/ or $efa =~ m/hardcover/ or $efa =~ m/hard cover/ or $efa =~ m/library/) 
        {    
            if (!defined $concat_edition_terms) { $concat_edition_terms = 'hardcover'; }
            else { $concat_edition_terms = $concat_edition_terms . ',hardcover'; }
        }
    if ($efa =~ m/paperback/ or $efa =~ m/paper back/ or $efa =~ m/softcover/ or $efa =~ m/soft cover/ or $efa =~ m/pb / or $efa =~ m/ pb/) 
        {    
            if (!defined $concat_edition_terms) { $concat_edition_terms = 'softcover'; }
            else { $concat_edition_terms = $concat_edition_terms . ',softcover'; }
        }
    if ($efa =~ m/mass/) 
        {    
            if (!defined $concat_edition_terms) { $concat_edition_terms = 'massmarket'; }
            else { $concat_edition_terms = $concat_edition_terms . ',massmarket'; }
        }
}

my @pubdates = $marc->field('260');
my @norm_pubdates;
if (scalar @pubdates > 0) { @norm_pubdates = norm_pubdates(@pubdates); }
my $concat_pubdates = '';
foreach my $pubdate (@norm_pubdates) { $concat_pubdates = $concat_pubdates . ',' . $pubdate; }

my @rda_pubdates = $marc->field('264');
my @norm_rda_pubdates;
if (scalar @rda_pubdates > 0) { @norm_rda_pubdates = norm_rda_pubdates(@rda_pubdates); }
my $concat_rda_pubdates = '';
foreach my $rda_pubdate (@norm_rda_pubdates) { $concat_rda_pubdates = $concat_rda_pubdates . ',' . $rda_pubdate; }

my @publishers = $marc->field('260');
my @norm_publishers;
if (scalar @publishers > 0) { @norm_publishers = norm_publishers(@publishers); }
my $concat_publishers = '';
foreach my $publisher (@norm_publishers) { $concat_publishers = $concat_publishers . ',' . $publisher; }

my @rda_publishers = $marc->field('264');
my @norm_rda_publishers;
if (scalar @rda_publishers > 0) { @norm_rda_publishers = norm_rda_publishers(@rda_publishers); }
my $concat_rda_publishers = '';
foreach my $rda_publisher (@norm_rda_publishers) { $concat_rda_publishers = $concat_rda_publishers . ',' . $rda_publisher; }

my @descrip = $marc->field('300');
my @norm_descriptions = norm_descriptions(@descrip);
my $description;
my %description_elements = (
    pages => 0,
    plates => 0,
    minutes => 0,
    discs => 0,
    tapes => 0,
    score => 0,
    volume => 0,
    cartridge => 0,
    centimeter => 0
);
my $color = 0;
my $bw = 0;
foreach my $nm (@norm_descriptions) {
    my @elements = split /(\d+)/, $nm;
    my $store_number = 0;
    foreach my $e (@elements) {
        if ($e =~ /^\d+$/) { $store_number = $e; next; }
        if ($e =~ m/color/) { $color = 1; }
        if ($e =~ m/black/ and $e =~ m/white/) { $bw = 1; }
        if ($e =~ m/b&w/ or $e =~ m/b & w/ or $e =~ m/b and w/) { $bw = 1; }
        if ($store_number > 0) {
            if ($e eq 'page') { $description_elements{pages} += $store_number; } 
            if ($e eq 'p')    { $description_elements{pages} += $store_number; }
            if ($e eq 'pg')   { $description_elements{pages} += $store_number; }
            if ($e eq 'hour' or $e eq 'hr')  { $description_elements{minutes} += ($store_number * 60); }
            if ($e eq 'centimeter' or $e eq 'cm') { $description_elements{centimeter} += $store_number; }
        }
    }
}
if ($color == 1 and $bw == 0) { $description = 'cl'; }
if ($color == 0 and $bw == 1) { $description = 'bw'; }
if ($color == 0 and $bw == 0) { $description = 'un'; }
if ($color == 1 and $bw == 1) { $description = 'un'; } 
$description = $description . 'pg' . sprintf("%08d", $description_elements{pages});
$description = $description . 'pl' . sprintf("%08d", $description_elements{plates});
$description = $description . 'mn' . sprintf("%08d", $description_elements{minutes});
$description = $description . 'dc' . sprintf("%08d", $description_elements{discs});
$description = $description . 'tp' . sprintf("%08d", $description_elements{tapes});
$description = $description . 'sc' . sprintf("%08d", $description_elements{score});
$description = $description . 'vl' . sprintf("%08d", $description_elements{volume});
$description = $description . 'ct' . sprintf("%08d", $description_elements{cartridge});
$description = $description . 'cn' . sprintf("%08d", $description_elements{centimeter});

my @added_entries = $marc->field('700');
my @norm_added_entries;
if (scalar @added_entries > 0) { @norm_added_entries = norm_added_entries(@added_entries); }
my $concat_added_entries = '';
foreach my $ae (@norm_added_entries) {
   if (!defined $concat_added_entries) { $concat_added_entries = $ae; }
    else { $concat_added_entries = $concat_added_entries . ',' . $ae; }
}

my $added_entry_score = 2;
my $linked_entry_score = 1;
my $issn_score = 1;
my $isbn_score = 1;
my $ost_score = 1;
my $authority_score = 1;  #subfield 0s
my $note_score = 1;
my $description_score = 1;
my $author_score = 1;
my $edition_score = 1;
my $title_score = 1;
my $heading_score = 1;

my @subjects = $marc->field('6..');  #two points
my @norm_subjects = norm_subjects(\@subjects,$score_method);
my $subjects_score = score_subjects(\@norm_subjects,$score_method);

my @osts = $marc->field('024');
my @notes = $marc->field('5..');

my @titles = $marc->field('210');
push @titles, $marc->field('222');
push @titles, $marc->field('24.');
my @title_subfields;
foreach my $f (@titles) {
    push @title_subfields, $f->subfield('a','b','c','f','g','k','n','p','s','0');
}

my @editions = $marc->field('25.');
push @editions, $marc->field('26.');
push @editions, $marc->field('27.');
my $edition_length = 0 + ((scalar @editions) * $edition_score);
if ($edition_length > 5) {$edition_length = 5};

my @authors = $marc->field('100');
push @authors, $marc->field('110');
push @authors, $marc->field('111');
push @authors, $marc->field('130');
my @author_subfields;
foreach my $f (@authors) {
    push @author_subfields, $f->subfield('a','b','c','d','e','f','g','j','k','l','n','p','q','t','u');
}

my @descriptions = $marc->field('3..');
push @descriptions, $marc->field('505');
push @descriptions, $marc->field('520');
my $description_length = 0 + ((scalar @descriptions) * $description_score);
if ($description_length > 5) {$description_length = 5};

undef @added_entries;
push @added_entries, $marc->field('70.');  #two points
push @added_entries, $marc->field('71.');
push @added_entries, $marc->field('72.');
push @added_entries, $marc->field('73.');
push @added_entries, $marc->field('74.');
push @added_entries, $marc->field('75.');
push @added_entries, $marc->field('80.');
push @added_entries, $marc->field('81.');
push @added_entries, $marc->field('83.');

my @linked_entries = $marc->field('76.'); #one points
push @linked_entries, $marc->field('77.');
push @linked_entries, $marc->field('78.');

my @authorities_maybe = $marc->field('100');
push @authorities_maybe, $marc->field('110');
push @authorities_maybe, $marc->field('111');
push @authorities_maybe, $marc->field('130');
push @authorities_maybe, $marc->field('6..');
push @authorities_maybe, $marc->field('7..');
push @authorities_maybe, $marc->field('830');
my @authorities;
foreach my $a (@authorities_maybe) {
    push @authorities, $a->subfield('0');
}

my @headings = $marc->field('3..');
push @headings, $marc->field('046');
push @headings, $marc->field('18.');
push @headings, $marc->field('162');
push @headings, $marc->field('15.');
push @headings, $marc->field('14.');
my $heading_length = 0 + ((scalar @headings) * $heading_score);
if ($heading_length > 5) {$heading_length = 5};

my $score = 0
    + $subjects_score
    + ((scalar @isbns) * $isbn_score)
    + ((scalar @issns) * $issn_score)
    + ((scalar @osts) * $ost_score)
    + ((scalar @linked_entries) * $linked_entry_score)
    + ((scalar @added_entries) * $added_entry_score)
    + ((scalar @authorities) * $authority_score)
    #notes not currently added, max out if used?
    + $description_length
    + ((scalar @author_subfields) * $author_score)
    + $edition_length
    + ((scalar @title_subfields) * $title_score)
    + $heading_length
;
if (defined $concat_rda_pubdates and $concat_rda_pubdates ne '') { $score = $score + 3; }

return [$subjects_score
    ,((scalar @isbns) * $isbn_score)
    ,((scalar @issns) * $issn_score)
    ,((scalar @osts) * $ost_score)
    ,((scalar @linked_entries) * $linked_entry_score)
    ,((scalar @added_entries) * $added_entry_score)
    ,((scalar @authorities) * $authority_score)
    ,$description_length
    ,((scalar @author_subfields) * $author_score)
    ,$edition_length
    ,((scalar @title_subfields) * $title_score)
    ,$heading_length
    ,$score
    ,$title_str
    ,$subtitle_str
    ,$author_str
    ,$concat_upcs
    ,$concat_isbns
    ,$concat_issns
    ,$concat_pubdates
    ,$concat_rda_pubdates
    ,$concat_oclcs
    ,$description
    ,$titlepart_str
    ,$titlepartname_str
    ,$concat_publishers
    ,$concat_rda_publishers
    ,$concat_edition_terms
    ,$concat_added_entries
];



sub norm_isbn {
    my $str = shift;
    my $norm = '';
    return '' unless defined $str;
    #$str =~ s/-//g;
    #$str =~ s/^\s+//;
    #$str =~ s/\s+$//;
    #$str =~ s/\s+//g;
    #$str =~ s/://g;
    $str =~ tr/xX0-9//cd;
    if (length($str) < 8 or !defined $str) { return ''; }
    $str = lc $str;
    my $isbn;
    if ($str =~ /^(\d{12}[0-9-x])/) {
        $isbn = $1;
        $norm = $isbn;
    } elsif ($str =~ /^(\d{9}[0-9x])/) {
        $isbn =  Business::ISBN->new($1);
        my $isbn13 = $isbn->as_isbn13;
        $norm = lc($isbn13->as_string);
        $norm =~ s/-//g;
    }
    return $norm;
}

sub norm_string {
    my $str = shift;
    return '' unless defined $str;
    $str = lc $str;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    $str =~ s/,//g;
    $str =~ s/[[:punct:]]//g;
    if (length($str) < 1) { return ''; }
    return $str;
}

sub norm_oclc {
    my $str = shift;
    return '' unless defined $str;
    $str = lc $str;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    $str =~ s/[^0-9a-z]//g;
    if (length($str) < 1) { return ''; }
    if ($str =~ /ocolc(.*?)\d{7,9}[0-9]/ or $str =~ /ocm(.*?)\d{7}[0-9]/ or $str =~ /ocn(.*?)\d{8}[0-9]/ or $str =~ /on(.*?)\d{9}[0-9]/)
            { return $str; } else { return ''; }
    return $str;
}

sub norm_issn {
    my $str = shift;
    return '' unless defined $str;
    $str = uc $str;
    $str =~ s/-//g;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    $str =~ s/[^0-9X]//g;
    my $v = is_issn_valid($str);
    if ($v != 1) { return ''; }
    my $issn = substr $str, 0, 7;
    my $checksum = calculate_issn_checksum($issn);
    my $norm = $issn . $checksum;
    return $norm;
}

sub norm_pubdate {
    my $str = shift;
    return '' unless defined $str;
    if ($str =~ m/c/) { return ''; }
    $str =~ s/[^0-9+]//g;
    return $str;
}

sub norm_rda_pubdate {
    my $str = shift;
    return '' unless defined $str;
    $str =~ s/[^0-9+]//g;
    return $str;
}

sub norm_publisher {
    my $str = shift;
    return '' unless defined $str;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    if ($str =~ m/c/) { return ''; }
    return lc($str);
}


sub calculate_issn_checksum {
    my $issn = shift;
    my $a = (substr $issn, 0, 1) * 8;
    my $b = (substr $issn, 1, 1) * 7;
    my $c = (substr $issn, 2, 1) * 6;
    my $d = (substr $issn, 3, 1) * 5;
    my $e = (substr $issn, 4, 1) * 4;
    my $f = (substr $issn, 5, 1) * 3;
    my $g = (substr $issn, 6, 1) * 2;
    my $i = 11 - (($a + $b + $c + $d + $e + $f + $g) % 11);
    if ($i == 10) { $i = 'X'; }
    return $i;
}

sub is_issn_valid {
    my $issn = shift;
    if (length($issn) > 8 or length($issn) < 7) { return 0; }
    if ($issn =~ m/^[0-9]{7}[0-9X]\z/) { return 1; }
    return 0;
}


sub norm_upcs {
    my @upcs = @_; 
    my %uniq_upcs = ();
    foreach my $field (@upcs) {
        my $ind1 = $field->indicator('1');
        if ($ind1 eq '0' or $ind1 eq '2' or $ind1 eq '3' or $ind1 eq '4' or $ind1 eq '7') { next; }
        my $sfa = $field->subfield('a');
        my $norm = norm_upc($sfa);
        $uniq_upcs{$norm}++ unless $norm eq '';
        my $sfz = $field->subfield('z');
        $norm = norm_upc($sfz);
        $uniq_upcs{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_upcs);
}

sub norm_upc {
    my $str = shift;
    my $norm = '';
    return '' unless defined $str;
    $str =~ s/-//g;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    if (length($str) < 1) { return ''; }
    $str = lc $str;
    $str =~ s/[^0-9]//g;    
    return $str;
}

sub norm_isbns {
    my @isbns = @_;
    my %uniq_isbns = ();
    foreach my $field (@isbns) {
        my $sfa = $field->subfield('a');
        my $norm = norm_isbn($sfa);
        $uniq_isbns{$norm}++ unless $norm eq '';
        my $sfz = $field->subfield('z');
        $norm = norm_isbn($sfz);
        $uniq_isbns{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_isbns);
}

sub norm_subjects {
    my $subjects = shift;
    my $score_method = shift;
    my %uniq_subjects = ();
    foreach my $field (@$subjects) {
        my $tag = $field->tag();
        my $sfa = $field->subfield('a');
        my $sf2 = $field->subfield('2');
        my $ind1 = $field->indicator(1);
        if ($ind1 eq '' or !defined $ind1) { $ind1 = '_'; }
        my $ind2 = $field->indicator(2);
        if ($ind2 eq '' or !defined $ind2) { $ind2 = '_'; }
        my $skip_flag = 0;
        if ($score_method eq 'lc primary') {
            if ($ind2 ne '0' and $ind2 ne '1' and $ind2 ne '2' and $ind2 ne '3' 
                and $ind2 ne '4' and $ind2 ne '5' and $ind2 ne '6') { $skip_flag = 1; }
        }
        if ($skip_flag == 1) { next; }
        my $norm_sfa = norm_string($sfa);
        my $norm_sf2 = norm_string($sf2);
        my $norm = '';    
        if ($norm_sfa ne '') {        
            $norm = $tag . $ind1 . $ind2 . '$a' . $norm_sfa . '$2' . $norm_sf2;
        }
        $uniq_subjects{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_subjects);
}

sub norm_added_entries {
    my @aes = shift;
    my %uniq_aes = ();
    foreach my $field (@aes) {
        my $sfa; 
        $sfa = $field->subfield('a');
        if (!defined $sfa) { next; }
        my $norm = norm_string($sfa);
        $uniq_aes{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_aes);
}

sub score_subjects {
    my $subjects = shift;
    my $score_method = shift;
    my $sub_count = scalar @$subjects;
    my $lc_bonus = 0;
    my $lc_genre_bonus = 0;
    if ($score_method eq 'lc primary') {
        foreach my $x (@{$subjects}) {
            my $q = substr $x, 4, 1;
            if ($q eq '0') { $lc_bonus++ };
        }
    }
    if ($score_method eq 'lc genre forms') {
        foreach my $x (@{$subjects}) {
            my $q = substr $x, 4, 1; 
            if ($q eq '0') { $lc_bonus++ };
            if ($x =~ m/2lcgft/) { $lc_genre_bonus++ }; 
        }
    }
    my $r;
    if ($score_method eq 'default') { $r = $sub_count * 2; }
    if ($score_method eq 'lc primary') { $r = $sub_count + $lc_bonus; }
    if ($score_method eq 'lc genre forms') { $r = $sub_count + $lc_bonus + $lc_genre_bonus; }
    if ($r > 20) { $r = 20; }
    return $r;
}

sub norm_oclcs {
    my @oclcs = @_;
    my %uniq_oclcs = ();
    foreach my $field (@oclcs) {
        my $sfa = $field->subfield('a');
        my $norm = norm_oclc($sfa);
        $uniq_oclcs{$norm}++ unless $norm eq '';
        my $sfz = $field->subfield('z');
        $norm = norm_oclc($sfz);
        $uniq_oclcs{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_oclcs);
}

sub norm_issns {
    my @issns = @_;
    my %uniq_issns = ();
    foreach my $field (@issns) {
        my $sfa = $field->subfield('a');
        my $norm = norm_issn($sfa);
        $uniq_issns{$norm}++ unless $norm eq '';
        my $sfz = $field->subfield('z');
        $norm = norm_issn($sfz);
        $uniq_issns{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_issns);
}

sub norm_pubdates {
    my @pubdates = shift;
    my %uniq_pubdates = ();
    foreach my $field (@pubdates) {
        my $sfc = $field->subfield('c');
        my @multiples;
        if ($sfc) { @multiples = split / /, $sfc; }
        my $norm;
        foreach my $x (@multiples) {        
            $norm = norm_pubdate($x);
            $uniq_pubdates{$norm}++ unless $norm eq '';
        }
    }
    return sort(keys %uniq_pubdates);
}

sub norm_rda_pubdates {
    my @rda_pubdates = @_;

    my %uniq_rda_pubdates = ();
    foreach my $field (@rda_pubdates) {
        my $ind2 = $field->indicator('2');
        if ($ind2 ne '1') { next; }
        my $sfc = $field->subfield('c');
        my @multiples;
        if ($sfc) { @multiples = split / /, $sfc; }
        my $norm; 
        foreach my $x (@multiples) {
            my $norm = norm_rda_pubdate($x);
            $uniq_rda_pubdates{$norm}++ unless $norm eq '';
        }  
    }
    return sort(keys %uniq_rda_pubdates);
}

sub norm_publishers {
    my @publishers = @_;
    my %uniq_publishers = ();
    foreach my $field (@publishers) {
        my $sfb = $field->subfield('b');
        my $norm = norm_publisher($sfb);
        $uniq_publishers{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_publishers);
}

sub norm_rda_publishers {
    my @rda_publishers = @_;
    my %uniq_rda_publishers = ();
    foreach my $field (@rda_publishers) {
        my $ind2 = $field->indicator('2');
        if ($ind2 ne '1') { next; }
        my $sfb = $field->subfield('b');
        my $norm = norm_string($sfb);
        $uniq_rda_publishers{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_rda_publishers);
}

sub norm_descriptions {
    my @descriptions = @_;
    my %uniq_descriptions = ();
    foreach my $field (@descriptions) {
        my $sfa = $field->subfield('a');
        my $norm = norm_string($sfa);
        $uniq_descriptions{$norm}++ unless $norm eq '';
    }
    foreach my $field (@descriptions) {
        my $sfa = $field->subfield('c');
        my $norm = norm_string($sfa);
        $uniq_descriptions{$norm}++ unless $norm eq '';
    }
    return sort(keys %uniq_descriptions);
}

$function$;

DROP FUNCTION IF EXISTS demerge_group(INTEGER);
CREATE OR REPLACE FUNCTION demerge_group(grp_id INTEGER)
    RETURNS BOOLEAN AS 
$BODY$
DECLARE 
    x   BIGINT;
BEGIN
    FOR x IN SELECT UNNEST(records) FROM groups WHERE id = grp_id LOOP
        PERFORM demerge_record(x);
    END LOOP;

    RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS demerge_record(BIGINT);
CREATE OR REPLACE FUNCTION demerge_record(bre_id BIGINT)
    RETURNS BOOLEAN AS
$BODY$ 
DECLARE
    acn_id   BIGINT;
    acnx_id  BIGINT;
    lib      INTEGER;
    pre      INTEGER;
    suf      INTEGER;
    lab      TEXT;  
    h         BIGINT;
BEGIN
    UPDATE biblio.record_entry SET deleted = FALSE WHERE id = bre_id;
    UPDATE action.hold_request SET target = bre_id WHERE id IN (SELECT UNNEST(holds) FROM bre_rollback_log WHERE record = bre_id)
        AND capture_time IS NULL AND fulfillment_time IS NULL AND cancel_time IS NULL and hold_type = 'T';
   
    FOR acn_id IN SELECT acn FROM acn_rollback_log WHERE original_record = bre_id LOOP
        acnx_id := NULL;
        -- check to see if a acn has taken the place of the old one 
        SELECT owning_lib, label, prefix, suffix FROM asset.call_number WHERE id = acn_id INTO lib, lab, pre, suf;
        SELECT id FROM asset.call_number
            WHERE id != acn_id AND suffix = suf AND prefix = pre AND label = lab AND owning_lib = lib
            AND record = bre_id AND deleted = FALSE
            INTO acnx_id;
        -- if it found a value then the call number has been replaced so move on, otherwise ....
        IF acnx_id IS NULL THEN
            -- repoint the call number to the correct record
            UPDATE asset.call_number SET record = bre_id, deleted = FALSE WHERE id = acn_id;
        END IF;
        -- make sure any volume holds that got moved to a new volume get moved back 
        UPDATE action.hold_request SET target = COALESCE(acnx_id,acn_id) WHERE hold_type = 'V' 
           AND id IN (SELECT UNNEST(holds) FROM acn_rollback_log WHERE acn = acn_id) 
           AND capture_time IS NULL AND fulfillment_time IS NULL AND cancel_time IS NULL
           AND target != COALESCE(acnx_id,acn_id);
        -- make sure copies are on the correct ACN, copy holds should never be moved so they aren't touched 
        UPDATE asset.copy SET call_number = COALESCE(acnx_id,acn_id) WHERE id IN (SELECT acp FROM acp_rollback_log WHERE acn = acn_id)
            AND call_number != COALESCE(acnx_id,acn_id);
    END LOOP;

    -- now let us make sure that parts get moved back to the right record, part holds and the copy map point to the part so they shouldn't need updating 
    UPDATE biblio.monograph_part SET record = bre_id WHERE id IN (SELECT monograph_part FROM monograph_part_rollback_log WHERE record = bre_id)
      AND record != bre_id;
    

    RETURN TRUE;
END;
$BODY$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS merge_next();
CREATE OR REPLACE FUNCTION merge_next()
    RETURNS INTEGER AS 
$BODY$
DECLARE
    next_id     INTEGER;
    r           INTEGER;
BEGIN

    SELECT id FROM groups WHERE done = FALSE LIMIT 1 INTO next_id;

    SELECT merge_group(next_id) INTO r;

    RETURN r;

END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS merge_group(INTEGER);
CREATE OR REPLACE FUNCTION merge_group(fg_id INTEGER)
    RETURNS INTEGER AS
$BODY$
DECLARE
    r           INTEGER;
    update_isbn BOOLEAN DEFAULT FALSE;
    update_upc  BOOLEAN DEFAULT FALSE;
    update_oclc BOOLEAN DEFAULT FALSE;
    update_500  BOOLEAN DEFAULT FALSE;
    create_tags BOOLEAN DEFAULT FALSE;
    del_binding BOOLEAN DEFAULT FALSE;
    lead_xml    TEXT;  
    feature_id  INTEGER;
    create_tag  TEXT;  
    create_sf   TEXT;
    create_txt  TEXT; 
    lead_id     BIGINT;
    isbns       TEXT[];
    upcs        TEXT[];
    oclcs       TEXT[];
    notes       TEXT[]; 
    BEGIN
        SELECT lead_record FROM groups WHERE id = fg_id INTO lead_id;
        RAISE INFO 'group is %', fg_id;
        RAISE INFO 'lead record is %', lead_id;
        UPDATE biblio.record_entry SET deleted = FALSE WHERE id = lead_id AND deleted = TRUE;

        IF dedupe_setting('remove binding statements') = 'TRUE' THEN
            del_binding := TRUE;
        END IF;
        IF dedupe_setting_exists('create single subfield tag') = 1 THEN
            create_tags := TRUE;
        END IF;
        IF dedupe_setting('keep secondary isbns') = 'TRUE' THEN
            update_isbn := TRUE;
        END IF;
        IF dedupe_setting('keep secondary upcs') = 'TRUE' THEN
            update_upc := TRUE;
        END IF;
        IF dedupe_setting('keep secondary oclcs') = 'TRUE' THEN
            update_oclc := TRUE;
        END IF;
        IF dedupe_setting('keep secondary 500s') = 'TRUE' THEN
            update_500 := TRUE;
        END IF;

        IF create_tags THEN
           FOR feature_id IN SELECT id FROM dedupe_features WHERE name = 'create single subfield tag' LOOP
               SELECT value, value2, value3 FROM dedupe_features WHERE id = feature_id INTO create_tag, create_sf, create_txt;
               SELECT * FROM insert_single_subfield_tag(create_tag, create_sf, create_txt, lead_xml) INTO lead_xml;
           END LOOP;
        END IF;

        FOR r IN SELECT UNNEST(records) FROM groups WHERE id = fg_id LOOP
            IF update_upc OR update_isbn OR update_oclc OR create_tags OR del_binding OR update_500 THEN
                SELECT marc FROM biblio.record_entry WHERE id = lead_id INTO lead_xml;
                SELECT isbn_values, upc_values, oclc_values FROM dedupe_batch WHERE record = r INTO isbns, upcs, oclcs;
            END IF;
            IF update_oclc AND oclcs IS NOT NULL THEN
                SELECT * FROM migrate_oclcs(oclcs,lead_xml) INTO lead_xml;
                RAISE INFO 'migrating oclcs are %', oclcs;
            END IF;
            IF update_isbn AND isbns IS NOT NULL THEN
                SELECT * FROM migrate_isbns(isbns,lead_xml) INTO lead_xml;
                RAISE INFO 'migrating isbns are %', isbns;
            END IF;
            IF update_upc AND upcs IS NOT NULL THEN
                SELECT * FROM migrate_upcs(upcs,lead_xml) INTO lead_xml;
                RAISE INFO 'migrating upcs are %', upcs;
            END IF;
            IF update_500 THEN
                SELECT OILS_XPATH('//*[@tag="500"]/*[@code="a"]/text()',marc) FROM biblio.record_entry WHERE id = r INTO notes;
                SELECT * FROM migrate_500s(notes,lead_xml) INTO lead_xml;
                RAISE INFO 'migrating notes are %', notes;
            END IF;
        END LOOP;
        IF del_binding THEN
            SELECT * FROM remove_binding_statements(lead_xml) INTO lead_xml;
        END IF;
        IF update_upc OR update_isbn OR update_oclc OR create_tags OR del_binding OR update_500 THEN
            UPDATE biblio.record_entry SET marc = lead_xml WHERE id = lead_id;
            PERFORM PG_SLEEP(1);
        END IF;

        FOR r IN SELECT UNNEST(records) FROM groups WHERE id = fg_id LOOP
            PERFORM log_asset_merges(id,r,lead_record) FROM groups WHERE id = fg_id AND lead_record <> r; 
            PERFORM asset.merge_record_assets(lead_record,r) FROM groups WHERE id = fg_id AND lead_record <> r;
        END LOOP;
        UPDATE groups SET done = TRUE WHERE id = fg_id;
        RETURN fg_id;
    END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migrate_500s(TEXT[], TEXT);
CREATE OR REPLACE FUNCTION migrate_500s(note_array TEXT[], merge_to text)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $notes = shift;
my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    return;
}
my @lead_500_fields = $to_marc->field('500');
my @lead_500s;
foreach my $l (@lead_500_fields) {
    my $a = $l->subfield('a');
    if ($a) { push @lead_500s, $a; }
}
my @unique = do { my %seen; grep { !$seen{$_}++ } @lead_500s };

foreach my $n (@$notes) {
    #check to see $n is in @unique and if so skip it
    if ( grep( /^$n$/, @unique ) ) { next; }
    my $field = MARC::Field->new( '500', '', '', 'a' => $n);
    $to_marc->insert_fields_ordered($field);
}

return $to_marc->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migrate_oclcs(TEXT[], TEXT);
CREATE OR REPLACE FUNCTION migrate_oclcs(oclc_array TEXT[], merge_to text)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict; 
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $oclcs = shift;
my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    return;
}

my @lead_035_fields = $to_marc->field('035');
my @lead_035s;
foreach my $l (@lead_035_fields) {
    my $a = $l->subfield('a');
    my $z = $l->subfield('z');
    if ($a) { push @lead_035s, $a; }
    if ($z) { push @lead_035s, $z; }
}
my @unique = do { my %seen; grep { !$seen{$_}++ } @lead_035s };

foreach my $oclc (@$oclcs) {
    #check to see $oclc is in @unique and if so skip it
    if ( grep( /^$oclc$/, @unique ) ) { next; }
    my $valid = test_oclc($oclc);
    if ($valid == 0) { next; }
    my $field = MARC::Field->new( '035', '', '', 'a' => $oclc);
    $to_marc->insert_fields_ordered($field);
}

return $to_marc->as_xml_record();

sub test_oclc {
    my $str = shift;
    my $valid = 0;
    $str = lc $str;
    $str =~ s/-//g;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    if ($str =~ /ocolc(.*?)\d{7,9}[0-9]/ or $str =~ /ocm(.*?)\d{7}[0-9]/ or $str =~ /ocn(.*?)\d{8}[0-9]/ or $str =~ /on(.*?)\d{9}[0-9]/) 
        { $valid = 1; }
    return $valid;
}

$function$;

DROP FUNCTION IF EXISTS migrate_isbns(TEXT[], TEXT);
CREATE OR REPLACE FUNCTION migrate_isbns(isbn_array TEXT[], merge_to text)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $isbns = shift;
my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    return;
}

my @lead_020_fields = $to_marc->field('020');
my @lead_020s;
foreach my $l (@lead_020_fields) {
    my $a = $l->subfield('a');
    my $z = $l->subfield('z');
    if ($a) { push @lead_020s, $a; }
    if ($z) { push @lead_020s, $z; }
}
my @unique = do { my %seen; grep { !$seen{$_}++ } @lead_020s };

foreach my $isbn (@$isbns) {
    #check to see $isbn is in @unique and if so skip it 
    if ( grep( /^$isbn$/, @unique ) ) { next; }
    my $field = MARC::Field->new( '020', '', '', 'a' => $isbn);
    $to_marc->insert_fields_ordered($field);
}

return $to_marc->as_xml_record(); 

$function$;

DROP FUNCTION IF EXISTS migrate_upcs(TEXT[], TEXT);
CREATE OR REPLACE FUNCTION migrate_upcs(upc_array TEXT[], merge_to text)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $upcs = shift;
my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};  
if ($@) {
    #elog("could not parse: $@\n");
    return;
}   

my @lead_024_fields = $to_marc->field('024');
my @lead_024s;
foreach my $l (@lead_024_fields) {
    my $a = $l->subfield('a');
    my $z = $l->subfield('z');
    if ($a) { push @lead_024s, $a; }
    if ($z) { push @lead_024s, $z; }
}   
my @unique = do { my %seen; grep { !$seen{$_}++ } @lead_024s };

foreach my $upc (@$upcs) {
    #check to see $upc is in @unique and if so skip it 
    if ( grep( /^$upc$/, @unique ) ) { next; }
    my $field = MARC::Field->new( '024', '', '', 'a' => $upc);
    $to_marc->insert_fields_ordered($field);
}

return $to_marc->as_xml_record();

$function$;

CREATE OR REPLACE FUNCTION anyarray_agg_statefunc(state anyarray, value anyarray)
        RETURNS anyarray AS
$BODY$
        SELECT array_cat($1, $2)
$BODY$
        LANGUAGE sql IMMUTABLE;

DROP AGGREGATE IF EXISTS anyarray_agg(anyarray);
CREATE AGGREGATE anyarray_agg(anyarray) (
        SFUNC = anyarray_agg_statefunc,
        STYPE = anyarray
);

DROP FUNCTION IF EXISTS anyarray_sort(anyarray);
CREATE OR REPLACE FUNCTION anyarray_sort(with_array anyarray)
    RETURNS anyarray AS
$BODY$
    DECLARE
        return_array with_array%TYPE := '{}';
    BEGIN
        SELECT ARRAY_AGG(sorted_vals.val) AS array_value
        FROM
            (   SELECT UNNEST(with_array) AS val
                ORDER BY val
            ) AS sorted_vals INTO return_array;
        RETURN return_array;
    END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS anyarray_uniq(anyarray);
CREATE OR REPLACE FUNCTION anyarray_uniq(with_array anyarray)
    RETURNS anyarray AS
$BODY$
    DECLARE
        -- The variable used to track iteration over "with_array".
        loop_offset integer;

        -- The array to be returned by this function.
        return_array with_array%TYPE := '{}';
    BEGIN
        IF with_array IS NULL THEN
            return NULL;
        END IF;

        IF with_array = '{}' THEN
            return return_array;
        END IF;

        -- Iterate over each element in "concat_array".
        FOR loop_offset IN ARRAY_LOWER(with_array, 1)..ARRAY_UPPER(with_array, 1) LOOP
            IF with_array[loop_offset] IS NULL THEN
                IF NOT EXISTS
                    ( SELECT 1 FROM UNNEST(return_array) AS s(a)
                    WHERE a IS NULL )
                THEN return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
                END IF;
            -- When an array contains a NULL value, ANY() returns NULL instead of FALSE...
            ELSEIF NOT(with_array[loop_offset] = ANY(return_array)) OR NOT(NULL IS DISTINCT FROM (with_array[loop_offset] = ANY(return_array))) THEN
                return_array = ARRAY_APPEND(return_array, with_array[loop_offset]);
            END IF;
        END LOOP;

    RETURN return_array;
 END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS array_remove_clear_empty(anyarray);
CREATE OR REPLACE FUNCTION array_remove_clear_empty(from_array anyarray)
RETURNS anyarray AS
$BODY$
    BEGIN
        RETURN ARRAY_REMOVE(from_array, ARRAY['']);
    END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS review_group(BIGINT);
CREATE OR REPLACE function review_group(leadrecord BIGINT) 
RETURNS TABLE (
    type TEXT
    ,record BIGINT
    ,title TEXT
    ,subtitle TEXT
    ,title_part TEXT
    ,title_part_name TEXT
    ,author TEXT
) AS 
$BODY$
    BEGIN
    RETURN QUERY 
        SELECT * FROM (
            SELECT 'sub' AS type, a.record::BIGINT, a.o_title, a.o_subtitle, a.o_titlepart, a.o_titlepartname, a.o_author FROM dedupe_batch a 
                WHERE a.record IN (SELECT UNNEST(records) FROM groups WHERE lead_record = leadrecord)
            UNION ALL 
            SELECT 'lead' AS type, b.record::BIGINT, b.o_title, b.o_subtitle, b.o_titlepart, b.o_titlepartname, b.o_author FROM dedupe_batch b 
                WHERE b.record = leadrecord
        ) x ORDER BY 1, 2;
    END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS insert_single_subfield_tag(TEXT, TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION insert_single_subfield_tag(set_tag TEXT, set_subfield TEXT, set_value TEXT, merge_to TEXT)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict; 
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $set_tag = shift;
my $set_subfield = shift;
my $set_value = shift;
my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    return;
}

my $created_tag = MARC::Field->new( $set_tag, '1', '0', $set_subfield => $set_value );
$to_marc->insert_fields_ordered($created_tag);

return $to_marc->as_xml_record();

$function$;

CREATE OR REPLACE FUNCTION remove_binding_statements(merge_to TEXT)
 RETURNS text
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $to_xml = shift;

$to_xml =~ s/(<leader>.........)./${1}a/;
my $to_marc;
eval {
    $to_marc = MARC::Record->new_from_xml($to_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    return;
}

my @bindings = $to_marc->field('250');
foreach my $b (@bindings) {
    my $a = $b->subfield('a');
    $a = lc($a);
    if ($a) {
        if ($a =~ m/wraparound/
        or $a =~ m/hardcover/
        or $a =~ m/softcover/
        or $a =~ m/widescreen/
        or $a =~ m/fullscreen/
        or $a =~ m/book club edition/
        or $a =~ m/bookclub edition/
        or $a =~ m/first edition/
        or $a =~ m/second edition/
        or $a =~ m/revised edition/
        or $a =~ m/updated/ )
        { $to_marc->delete_field($b); }
    }
}

return $to_marc->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS get_dedupe_percent();
CREATE OR REPLACE FUNCTION get_dedupe_percent()
RETURNS NUMERIC AS
$BODY$
DECLARE
    migration      BOOLEAN DEFAULT FALSE;
    subordinates   BIGINT  DEFAULT 0;
    pool           BIGINT  DEFAULT 0;
    calced_percent NUMERIC(6,2);
BEGIN
    SELECT EXISTS (SELECT 1 FROM dedupe_features WHERE name = 'dedupe_type' AND value IN ('subset','migration')) 
        INTO migration;
    SELECT SUM(ARRAY_LENGTH(records,1)) FROM groups INTO subordinates;
    IF migration = true THEN 
        SELECT COUNT(*) FROM m_biblio_record_entry_legacy WHERE x_migrate INTO pool;
    ELSE 
        SELECT COUNT(*) FROM biblio.record_entry WHERE NOT deleted INTO pool;
    END IF;
    SELECT ((100::NUMERIC / pool::NUMERIC) * subordinates)::NUMERIC(6,2) INTO calced_percent;
    RETURN calced_percent;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS csv_wrap(TEXT);
CREATE OR REPLACE FUNCTION csv_wrap(str TEXT)
 RETURNS TEXT
 LANGUAGE plpgsql
AS $function$
BEGIN
    str := REGEXP_REPLACE(str, E'[\\n\\r]+', ' ', 'g' );
    str := REPLACE(str,'"','""');
    str := CONCAT_WS('','"',str,'"');
    RETURN str;
END;
$function$;
