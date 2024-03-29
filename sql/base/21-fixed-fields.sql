DROP TABLE IF EXISTS migration_tools.search_format_map;
CREATE TABLE migration_tools.search_format_map (code TEXT, itype CHAR(1), iform CHAR(1), phy CHAR(1), phyv CHAR(1), phyp SMALLINT,
    biblevel CHAR(1), iform_exclude CHAR(1)[], srform_exclude CHAR(1)[] );
INSERT INTO migration_tools.search_format_map (code, itype, iform, phy, phyv, phyp, biblevel, iform_exclude, srform_exclude) VALUES
    --                  itype iform phy   phyv  phyp  bib   itemform exclude     sr format exclude
     ('blu-ray',        'g',  NULL, 'v',  's',  4,    NULL, NULL,                NULL)
    ,('book',           'a',  NULL, NULL, NULL, NULL, 'a',  '{a,b,c,f,o,q,r,s}', NULL)
    ,('braille',        'a',  'f',  NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('casaudiobook',   'i',  NULL, 's',  'l',  3,    NULL, NULL,                NULL)
    ,('casmusic',       'j',  NULL, 's',  'l',  3,    NULL, NULL,                NULL)
    ,('cdaudiobook',    'i',  NULL, 's',  'f',  3,    NULL, NULL,                NULL)
    ,('cdmusic',        'j',  NULL, 's',  'f',  3,    NULL, NULL,                NULL)
    ,('dvd',            'g',  NULL, 'v',  'v',  4,    NULL, NULL,                NULL)
    ,('eaudio',         'i',  'o',  NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('ebook',          'a',  's',  NULL, NULL, NULL, 'a' , NULL,                NULL)
    ,('electronic',     's',  'o',  NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('equip',          'r',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('evideo',         'g',  'o',  NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('kit',            'o',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('lpbook',         'a',  'd',  NULL, NULL, NULL, 'a' , NULL,                NULL)
    ,('map',            'e',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('microform',      'a',  'b',  NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('music',          'j',  NULL, NULL, NULL, NULL, NULL, NULL,                '{a,b,c,d,e,f}')
    ,('phonomusic',     'j',  NULL, 's',  'a',  3,    NULL, NULL,                NULL)
    ,('phonospoken',    'i',  NULL, 's',  'a',  3,    NULL, NULL,                NULL)
    ,('picture',        'k',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('preloadedaudio', 'i',  'q',  NULL, NULL, NULL, NULL, NULL,                '{a,b,c,d,e,f,s}')
    ,('rpbook',         'a',  'd',  NULL, NULL, NULL, 'a' , NULL,                NULL)
    ,('score',          'c',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('serial',         NULL, NULL, NULL, NULL, NULL, 's' , NULL,                NULL)
    ,('software',       'm',  NULL, NULL, NULL, NULL, NULL, NULL,                NULL)
    ,('vhs',            'g',  NULL, 'v',  'b',  4,    NULL, NULL,                NULL)
;

CREATE OR REPLACE FUNCTION migration_tools.reingest_staged_record_attributes (rid BIGINT, pattr_list TEXT[] DEFAULT NULL, prmarc TEXT DEFAULT NULL, rdeleted BOOL DEFAULT TRUE) RETURNS INTEGER[] AS $func$
DECLARE
    transformed_xml TEXT;
    rmarc           TEXT := prmarc;
    tmp_val         TEXT;
    prev_xfrm       TEXT;
    normalizer      RECORD;
    xfrm            config.xml_transform%ROWTYPE;
    attr_vector     INT[] := '{}'::INT[];
    attr_vector_tmp INT[];
    attr_list       TEXT[] := pattr_list;
    attr_value      TEXT[];
    norm_attr_value TEXT[];
    tmp_xml         TEXT;
    tmp_array       TEXT[];
    attr_def        config.record_attr_definition%ROWTYPE;
    ccvm_row        config.coded_value_map%ROWTYPE;
    jump_past       BOOL;
BEGIN

    IF attr_list IS NULL OR rdeleted THEN -- need to do the full dance on INSERT or undelete
        SELECT ARRAY_AGG(name) INTO attr_list FROM config.record_attr_definition
        WHERE ( 
            tag IS NOT NULL OR 
            fixed_field IS NOT NULL OR
            xpath IS NOT NULL OR
            phys_char_sf IS NOT NULL OR
            composite
        ) AND (
            filter OR sorter
        );
    END IF;
    IF rmarc IS NULL THEN
        SELECT marc INTO rmarc FROM m_biblio_record_entry_legacy WHERE id = rid;
    END IF;

    FOR attr_def IN SELECT * FROM config.record_attr_definition WHERE NOT composite AND name = ANY( attr_list ) ORDER BY format LOOP

        jump_past := FALSE; -- This gets set when we are non-multi and have found something
        attr_value := '{}'::TEXT[];
        norm_attr_value := '{}'::TEXT[];
        attr_vector_tmp := '{}'::INT[];

        SELECT * INTO ccvm_row FROM config.coded_value_map c WHERE c.ctype = attr_def.name LIMIT 1;

        IF attr_def.tag IS NOT NULL THEN -- tag (and optional subfield list) selection
            SELECT  ARRAY_AGG(value) INTO attr_value
              FROM  (SELECT * FROM metabib.full_rec ORDER BY tag, subfield) AS x
              WHERE record = rid
                    AND tag LIKE attr_def.tag
                    AND CASE
                        WHEN attr_def.sf_list IS NOT NULL
                            THEN POSITION(subfield IN attr_def.sf_list) > 0
                        ELSE TRUE
                    END
              GROUP BY tag
              ORDER BY tag;

            IF NOT attr_def.multi THEN
                attr_value := ARRAY[ARRAY_TO_STRING(attr_value, COALESCE(attr_def.joiner,' '))];
                jump_past := TRUE;
            END IF;
        END IF;

        IF NOT jump_past AND attr_def.fixed_field IS NOT NULL THEN -- a named fixed field, see config.marc21_ff_pos_map.fixed_field
            attr_value := attr_value || vandelay.marc21_extract_fixed_field_list(rmarc, attr_def.fixed_field);

            IF NOT attr_def.multi THEN
                attr_value := ARRAY[attr_value[1]];
                jump_past := TRUE;
            END IF;
        END IF;
        
                IF NOT jump_past AND attr_def.xpath IS NOT NULL THEN -- and xpath expression

                    SELECT INTO xfrm * FROM config.xml_transform WHERE name = attr_def.format;

                    -- See if we can skip the XSLT ... it's expensive
                    IF prev_xfrm IS NULL OR prev_xfrm <> xfrm.name THEN
                        -- Can't skip the transform
                        IF xfrm.xslt <> '---' THEN
                            transformed_xml := oils_xslt_process(rmarc,xfrm.xslt);
                        ELSE
                            transformed_xml := rmarc;
                        END IF;

                        prev_xfrm := xfrm.name;
                    END IF;

                    IF xfrm.name IS NULL THEN
                        -- just grab the marcxml (empty) transform
                        SELECT INTO xfrm * FROM config.xml_transform WHERE xslt = '---' LIMIT 1;
                        prev_xfrm := xfrm.name;
                    END IF;

                    FOR tmp_xml IN SELECT UNNEST(oils_xpath(attr_def.xpath, transformed_xml, ARRAY[ARRAY[xfrm.prefix, xfrm.namespace_uri]])) LOOP
                        tmp_val := oils_xpath_string(
                                        '//*',
                                        tmp_xml,
                                        COALESCE(attr_def.joiner,' '),
                                        ARRAY[ARRAY[xfrm.prefix, xfrm.namespace_uri]]
                                    );
                        IF tmp_val IS NOT NULL AND BTRIM(tmp_val) <> '' THEN
                            attr_value := attr_value || tmp_val;
                            EXIT WHEN NOT attr_def.multi;
                        END IF;
                    END LOOP;
                END IF;

                IF NOT jump_past AND attr_def.phys_char_sf IS NOT NULL THEN -- a named Physical Characteristic, see config.marc21_physical_characteristic_*_map
                    SELECT  ARRAY_AGG(m.value) INTO tmp_array
                      FROM  vandelay.marc21_physical_characteristics(rmarc) v
                            LEFT JOIN config.marc21_physical_characteristic_value_map m ON (m.id = v.value)
                      WHERE v.subfield = attr_def.phys_char_sf AND (m.value IS NOT NULL AND BTRIM(m.value) <> '')
                            AND ( ccvm_row.id IS NULL OR ( ccvm_row.id IS NOT NULL AND v.id IS NOT NULL) );

                    attr_value := attr_value || tmp_array;

                    IF NOT attr_def.multi THEN
                        attr_value := ARRAY[attr_value[1]];
                    END IF;

                END IF;

                -- apply index normalizers to attr_value
        FOR tmp_val IN SELECT value FROM UNNEST(attr_value) x(value) LOOP
            FOR normalizer IN
                SELECT  n.func AS func,
                        n.param_count AS param_count,
                        m.params AS params
                  FROM  config.index_normalizer n
                        JOIN config.record_attr_index_norm_map m ON (m.norm = n.id)
                  WHERE attr = attr_def.name
                  ORDER BY m.pos LOOP
                    EXECUTE 'SELECT ' || normalizer.func || '(' ||
                    COALESCE( quote_literal( tmp_val ), 'NULL' ) ||
                        CASE
                            WHEN normalizer.param_count > 0
                                THEN ',' || REPLACE(REPLACE(BTRIM(normalizer.params,'[]'),E'\'',E'\\\''),E'"',E'\'')
                                ELSE ''
                            END ||
                    ')' INTO tmp_val;

            END LOOP;
            IF tmp_val IS NOT NULL AND tmp_val <> '' THEN
                -- note that a string that contains only blanks
                -- is a valid value for some attributes
                norm_attr_value := norm_attr_value || tmp_val;
            END IF;
        END LOOP;

        IF attr_def.filter THEN
            -- Create unknown uncontrolled values and find the IDs of the values
            IF ccvm_row.id IS NULL THEN
                FOR tmp_val IN SELECT value FROM UNNEST(norm_attr_value) x(value) LOOP
                    IF tmp_val IS NOT NULL AND BTRIM(tmp_val) <> '' THEN
                        BEGIN -- use subtransaction to isolate unique constraint violations
                            INSERT INTO metabib.uncontrolled_record_attr_value ( attr, value ) VALUES ( attr_def.name, tmp_val );
                        EXCEPTION WHEN unique_violation THEN END;
                    END IF;
                END LOOP;

                SELECT ARRAY_AGG(id) INTO attr_vector_tmp FROM metabib.uncontrolled_record_attr_value WHERE attr = attr_def.name AND value = ANY( norm_attr_value );
            ELSE
                SELECT ARRAY_AGG(id) INTO attr_vector_tmp FROM config.coded_value_map WHERE ctype = attr_def.name AND code = ANY( norm_attr_value );
            END IF;

            -- Add the new value to the vector
            attr_vector := attr_vector || attr_vector_tmp;
        END IF;
    END LOOP;

    
        IF ARRAY_LENGTH(pattr_list, 1) > 0 THEN
            SELECT vlist INTO attr_vector_tmp FROM metabib.record_attr_vector_list WHERE source = rid;
            SELECT attr_vector_tmp - ARRAY_AGG(id::INT) INTO attr_vector_tmp FROM metabib.full_attr_id_map WHERE attr = ANY (pattr_list);
            attr_vector := attr_vector || attr_vector_tmp;
        END IF;

        -- On to composite attributes, now that the record attrs have been pulled.  Processed in name order, so later composite
        -- attributes can depend on earlier ones.
        PERFORM metabib.compile_composite_attr_cache_init();
        FOR attr_def IN SELECT * FROM config.record_attr_definition WHERE composite AND name = ANY( attr_list ) ORDER BY name LOOP

            FOR ccvm_row IN SELECT * FROM config.coded_value_map c WHERE c.ctype = attr_def.name ORDER BY value LOOP

                tmp_val := metabib.compile_composite_attr( ccvm_row.id );
                CONTINUE WHEN tmp_val IS NULL OR tmp_val = ''; -- nothing to do

                IF attr_def.filter THEN
                    IF attr_vector @@ tmp_val::query_int THEN
                        attr_vector = attr_vector + intset(ccvm_row.id);
                        EXIT WHEN NOT attr_def.multi;
                    END IF;
                END IF;

                IF attr_def.sorter THEN
                    IF attr_vector @@ tmp_val THEN
                        DELETE FROM metabib.record_sorter WHERE source = rid AND attr = attr_def.name;
                        INSERT INTO metabib.record_sorter (source, attr, value) VALUES (rid, attr_def.name, ccvm_row.code);
                    END IF;
                END IF;

            END LOOP;

        END LOOP;

        return attr_vector;
    END;
    $func$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration_tools.view_staged_vlist (rid BIGINT) RETURNS TABLE (r_ctype text, r_code text, r_value text) AS $func$
DECLARE
    search  TEXT[];
    icon    TEXT[];
    vlist   INTEGER[];
BEGIN
    SELECT migration_tools.reingest_staged_record_attributes(rid) INTO vlist;

    RETURN QUERY SELECT ctype, code, value FROM config.coded_value_map WHERE id IN (SELECT UNNEST(vlist));
END;
$func$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.stage_vlist (rid BIGINT) RETURNS VOID AS $func$
DECLARE 
	search	TEXT[];
	vlist	INTEGER[];
BEGIN
	SELECT migration_tools.reingest_staged_record_attributes(rid) INTO vlist;

	SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(vlist)) 
		AND ctype = 'search_format' INTO search;

	UPDATE m_biblio_record_entry_legacy SET x_search_format = search  WHERE id = rid;
END;
$func$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION migration_tools.show_staged_vlist (rid BIGINT) RETURNS TEXT[] AS $func$
DECLARE
    search  TEXT[];
    vlist   INTEGER[];
BEGIN
    SELECT migration_tools.reingest_staged_record_attributes(rid) INTO vlist;

    SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(vlist))
        AND ctype = 'search_format' INTO search;

	RETURN search;
END;
$func$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration_tools.postfix_vlist (rid BIGINT) RETURNS VOID AS $func$
DECLARE
    search  TEXT[];
    vlist   INTEGER[];
BEGIN
    SELECT migration_tools.reingest_staged_record_attributes(rid) INTO vlist;

    SELECT ARRAY_AGG(code) FROM config.coded_value_map WHERE id IN (SELECT UNNEST(vlist))
        AND ctype = 'search_format' INTO search;

    UPDATE m_biblio_record_entry_legacy SET x_after_search_format = search WHERE id = rid;
END;
$func$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration_tools.set_exp_sfs (rid BIGINT) RETURNS VOID AS $func$
DECLARE
    cms TEXT[];
    y   TEXT;
    w   TEXT[];
BEGIN
	SELECT circ_mods FROM m_biblio_record_entry_legacy WHERE id = rid INTO cms;
    IF cms IS NOT NULL THEN
    	FOREACH y IN ARRAY cms LOOP
        	w := w || (SELECT sf1 FROM circ_mod_to_sf_map WHERE circ_mod = y);
            w := w || (SELECT sf2 FROM circ_mod_to_sf_map WHERE circ_mod = y);
            w := w || (SELECT sf3 FROM circ_mod_to_sf_map WHERE circ_mod = y);
        END LOOP;
	UPDATE m_biblio_record_entry_legacy SET expected_sfs = w WHERE id = rid;
    END IF;
END;
$func$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS migration_tools.modify_staged_fixed_fields (BIGINT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.modify_staged_fixed_fields (bib_id BIGINT, xcode TEXT)
 RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    r           TEXT;
    xitype      CHAR(1);
    xiform      CHAR(1);
    xphy        CHAR(1);
    xphyv       CHAR(1);
    xphyp       SMALLINT;
    xbiblevel   CHAR(1);
    xiform_exclude      CHAR(1)[];
    xsrform_exclude     CHAR(1)[];
	yiform_exclude 		TEXT;
	ysrform_exclude     TEXT;
BEGIN
    SELECT itype, iform, phy, phyv, phyp, biblevel, iform_exclude, srform_exclude FROM migration_tools.search_format_map WHERE code = xcode
        INTO xitype, xiform, xphy, xphyv, xphyp, xbiblevel, xiform_exclude, xsrform_exclude;
	IF xiform_exclude IS NOT NULL THEN 
		yiform_exclude := ARRAY_TO_STRING(xiform_exclude,',');
	ELSE 
		yiform_exclude := '';
	END IF;
	IF xsrform_exclude IS NOT NULL THEN 
		ysrform_exclude := ARRAY_TO_STRING(xsrform_exclude,',');
	ELSE
		ysrform_exclude := '';
	END IF;
    SELECT migration_tools.modify_fixed_fields(marc,xcode,xitype,xiform,xphy,xphyv,xphyp,xbiblevel,yiform_exclude,ysrform_exclude) FROM m_biblio_record_entry_legacy WHERE id = bib_id INTO r;
    IF r = 'failed to parse marcxml' THEN 
        RETURN FALSE;
    ELSE 
        UPDATE m_biblio_record_entry_legacy SET marc = r WHERE id = bib_id;
        RETURN TRUE;
    END IF;
END;
$function$;

DROP FUNCTION IF EXISTS migration_tools.modify_fixed_fields (TEXT, TEXT, CHAR(1), CHAR(1), CHAR(1), CHAR(1), SMALLINT, CHAR(1), TEXT, TEXT);
CREATE OR REPLACE FUNCTION migration_tools.modify_fixed_fields (TEXT, TEXT, CHAR(1), CHAR(1), CHAR(1), CHAR(1), SMALLINT, CHAR(1), TEXT, TEXT)
RETURNS TEXT
 LANGUAGE plperlu
AS $function$

# assumption is that there should only be a single format per item

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');
use MARC::Field;
use Data::Dumper;

my ($marcxml, $code, $itype, $iform, $phy, $phyv, $phyp, $biblevel, $iform_exclude_temp, $srform_exclude_temp) = @_;
my $marc;
my @iform_exclude;
if ($iform_exclude_temp) { @iform_exclude = split /,/, $iform_exclude_temp; }
my @srform_exclude;
if ($srform_exclude_temp) { @srform_exclude = split /,/, $srform_exclude_temp; }

$marcxml =~ s/(<leader>.........)./${1}a/;
eval {  $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8'); };
if ($@) {
  import MARC::File::XML (BinaryEncoding => 'utf8');
  return 'failed to parse marcxml';
}

my $ldr = $marc->leader();
if ($itype) { substr($ldr,6,1) = $itype; } else { substr($ldr,6,1) = '|'; }
if ($biblevel) { substr($ldr,7,1) = $biblevel; } else { substr($ldr,7,1) = '|'; }
$marc->leader($ldr);

my $zedzedeight;
my $zze_str = '0000000000000000000000000000000000000000';
my $new_zze;
my $formchar;
$zedzedeight = $marc->field('008');
if ($zedzedeight) {
    $zze_str = $zedzedeight->data();
}
if (length($zze_str) < 30) {
    my $nneight = MARC::Field->new( 918, '1', '0', 'a' => $zze_str );
    $marc->insert_fields_ordered($nneight);
    $zze_str = '0000000000000000000000000000000000000000';
}
if ($itype eq 'e' or $itype eq 'g' or $itype eq 'k')
    { $formchar = substr($zze_str,29,1); }
    else { $formchar = substr($zze_str,23,1); }
if (@iform_exclude and $itype) {
    if ($itype eq 'e' or $itype eq 'g' or $itype eq 'k')  {      #visual materials
        if ($formchar ~~ @iform_exclude) { substr($zze_str,29,1) = '|'; }
    } else { if ($formchar ~~ @iform_exclude) { substr($zze_str,23,1) = '|'; } }
}
if ($iform) {
    if ($itype eq 'e' or $itype eq 'g' or $itype eq 'k') {      #visual materials
        substr($zze_str,29,1) = $iform;
    } else {
        substr($zze_str,23,1) = $iform;
    }
} else {
    if ($itype eq 'e' or $itype eq 'g' or $itype eq 'k') {      #visual materials
            substr($zze_str,29,1) = '|';
        } else {
            substr($zze_str,23,1) = '|';
        }
}

$new_zze = MARC::Field->new('008',$zze_str);
if ($zedzedeight) { $zedzedeight->replace_with($new_zze); } else
    { $marc->insert_fields_ordered($new_zze); }

my @todelzzsx = $marc->field('006');
#save the old 006s in 916 fields
foreach my $sx (@todelzzsx) {
    my $nfield = MARC::Field->new( 916, '1', '0', 'a' => $sx->data() );
    $marc->insert_fields_ordered($nfield);
}
$marc->delete_fields(@todelzzsx);

my $zzsx_str = '00000000000000000';
if ($iform) { substr($zzsx_str,6,1) = $iform; }
my $zedzedsix = MARC::Field->new('006', $zzsx_str);
$marc->insert_fields_ordered($zedzedsix);

my @todelzzsv = $marc->field('007');
#save the old 007s in 917 fields
foreach my $sv (@todelzzsv) {
    my $nfield = MARC::Field->new( 917, '1', '0', 'a' => $sv->data() );
    $marc->insert_fields_ordered($nfield);
}
$marc->delete_fields(@todelzzsv);

my $nn = MARC::Field->new( 919, '1', '0', 'a' => 'record modified by automated fixed field changes' );
$marc->insert_fields_ordered($nn);

my $zedzedseven;
my $zzs_str;
    if ($phy) {
            if ($phy eq 'o' or $phy eq 'q' or $phy eq 'z' or $phy eq 't') { $zzs_str = '00'; }
            if ($phy eq 's' or $phy eq 'c') { $zzs_str = '00000000000000'; }
            if ($phy eq 'r') { $zzs_str = '00000000000'; }
            if ($phy eq 'm') { $zzs_str = '00000000000000000000000'; }
            if ($phy eq 'a') { $zzs_str = '00000000'; }
            if ($phy eq 'd') { $zzs_str = '000000'; }
            if ($phy eq 'f') { $zzs_str = '0000000000'; }
            if ($phy eq 'g') { $zzs_str = '000000000'; }
            if ($phy eq 'h') { $zzs_str = '0000000000000'; }
            if ($phy eq 'k') { $zzs_str = '000000'; }
            if ($phy eq 'v') { $zzs_str = '000000000'; }
            substr($zzs_str,0,1) = $phy;
            substr($zzs_str,$phyp,1) = $phyv;
            $zedzedseven = MARC::Field->new('007', $zzs_str);
            $marc->insert_fields_ordered($zedzedseven);
    }
my $text_zzs;
my $text_zzs_str = 't0';
if ($code eq 'rpbook' or $code eq 'lpbook' or $code eq 'braille') {
    if ($code eq 'rpbook') { substr($text_zzs_str,1,1) == 'a'; }
    if ($code eq 'lpbook') { substr($text_zzs_str,1,1) == 'b'; } 
    if ($code eq 'braille') { substr($text_zzs_str,1,1) == 'c'; }  
    $text_zzs = MARC::Field->new('007', $text_zzs_str);
    $marc->insert_fields_ordered($text_zzs);
}


return $marc->as_xml_record;
$function$;

DROP FUNCTION IF EXISTS migration_tools.modify_biblio_fixed_fields (BIGINT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.modify_biblio_fixed_fields (bib_id BIGINT, xcode TEXT)
 RETURNS BOOLEAN
 LANGUAGE plpgsql
AS $function$
DECLARE
    r           TEXT;
    xitype      CHAR(1);
    xiform      CHAR(1);
    xphy        CHAR(1);
    xphyv       CHAR(1);
    xphyp       SMALLINT;
    xbiblevel   CHAR(1);
    xiform_exclude      CHAR(1)[];
    xsrform_exclude     CHAR(1)[];
    yiform_exclude      TEXT;
    ysrform_exclude     TEXT;
BEGIN
    SELECT itype, iform, phy, phyv, phyp, biblevel, iform_exclude, srform_exclude FROM migration_tools.search_format_map WHERE code = xcode
        INTO xitype, xiform, xphy, xphyv, xphyp, xbiblevel, xiform_exclude, xsrform_exclude;
    IF xiform_exclude IS NOT NULL THEN
        yiform_exclude := ARRAY_TO_STRING(xiform_exclude,',');
    ELSE
        yiform_exclude := '';
    END IF;
    IF xsrform_exclude IS NOT NULL THEN
        ysrform_exclude := ARRAY_TO_STRING(xsrform_exclude,',');
    ELSE
        ysrform_exclude := '';
    END IF;

    SELECT migration_tools.modify_fixed_fields(marc,xcode,xitype,xiform,xphy,xphyv,xphyp,xbiblevel,yiform_exclude,ysrform_exclude) FROM biblio.record_entry WHERE id = bib_id INTO r;
    IF r = 'failed to parse marcxml' THEN
        RETURN FALSE;
    ELSE
        UPDATE biblio.record_entry SET marc = r WHERE id = bib_id;
        RETURN TRUE;
    END IF;
END;
$function$;
