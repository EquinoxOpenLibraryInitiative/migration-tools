CREATE OR REPLACE FUNCTION migration_tools.attempt_hz_date (TEXT,TEXT) RETURNS DATE AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output DATE;
    BEGIN
        FOR output IN
            EXECUTE E'SELECT (\'1970-01-01\'::date + \'' || attempt_value || E' days\'::interval)::date AS a;'
        LOOP
            RETURN output;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::date AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.attempt_sierra_timestamp (TEXT,TEXT) RETURNS TIMESTAMP AS $$
    DECLARE
        attempt_value ALIAS FOR $1;
        fail_value ALIAS FOR $2;
        output TIMESTAMP;
    BEGIN
            output := REGEXP_REPLACE(attempt_value,E'^(..)(..)(..)(..)(..)$',E'20\\1-\\2-\\3 \\4:\\5')::TIMESTAMP;
            RETURN output;
    EXCEPTION
        WHEN OTHERS THEN
            FOR output IN
                EXECUTE 'SELECT ' || quote_literal(fail_value) || '::TIMESTAMP AS a;'
            LOOP
                RETURN output;
            END LOOP;
    END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

CREATE OR REPLACE FUNCTION migration_tools.openbiblio2marc (x_bibid TEXT) RETURNS TEXT AS $func$
BEGIN
-- Expects the following table/columns:

-- export_biblio_tsv:
-- l_bibid               | 1
-- l_create_dt           | 2007-03-07 09:03:09
-- l_last_change_dt      | 2015-01-23 11:18:54
-- l_last_change_userid  | 2
-- l_material_cd         | 10
-- l_collection_cd       | 13
-- l_call_nmbr1          | Canada
-- l_call_nmbr2          | ON
-- l_call_nmbr3          | Ottawa 18
-- l_title               | Art and the courts : France ad England
-- l_title_remainder     | from 1259-1328
-- l_responsibility_stmt |
-- l_author              | National Gallery of Canada
-- l_topic1              |
-- l_topic2              |
-- l_topic3              |
-- l_topic4              |
-- l_topic5              |
-- l_opac_flg            | Y
-- l_flag_attention      | 0

-- export_biblio_field_tsv:
-- l_bibid       | 1
-- l_fieldid     | 1
-- l_tag         | 720
-- l_ind1_cd     | N
-- l_ind2_cd     | N
-- l_subfield_cd | a
-- l_field_data  | Brieger, Peter Henry

-- Map export_biblio_tsv as follows:
-- l_call_nmbr?             -> 099a
-- l_author                 -> 100a
-- l_title                  -> 245a
-- l_title_remainder        -> 245b
-- l_responsibility_stmt    -> 245c
-- l_topic?                 -> 650a
-- l_bibid                  -> 001

RETURN
    migration_tools.make_stub_bib(y.tag,y.ind1,y.ind2,y.data)
FROM (
    select
        array_agg(lpad(l_tag,3,'0') || l_subfield_cd) as "tag",
        array_agg(l_ind1_cd) as "ind1",
        array_agg(l_ind2_cd) as "ind2",
        array_agg(l_field_data) as "data"
    from (
        select
            l_tag,
            l_subfield_cd,
            l_ind1_cd,
            l_ind2_cd,
            l_field_data
        from export_biblio_field_tsv
        where l_bibid = x_bibid
    union
        select
            '099' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            concat_ws(' ',
                nullif(btrim(l_call_nmbr1),''),
                nullif(btrim(l_call_nmbr2),''),
                nullif(btrim(l_call_nmbr3),'')
            ) as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid
    union
        select
            '100' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_author as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_author),'') is not null
    union
        select
            '245' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_title as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_title),'') is not null
    union
        select
            '245' as "l_tag",
            'b' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_title_remainder as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_title_remainder),'') is not null
    union
        select
            '650' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_topic1 as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_topic1),'') is not null
    union
        select
            '650' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_topic2 as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_topic2),'') is not null
    union
        select
            '650' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_topic3 as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_topic3),'') is not null
    union
        select
            '650' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_topic4 as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_topic4),'') is not null
    union
        select
            '650' as "l_tag",
            'a' as "l_subfield_cd",
            ' ' as "l_ind1_cd",
            ' ' as "l_ind2_cd",
            l_topic5 as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid and nullif(btrim(l_topic5),'') is not null
    union
        select
            '001' as "l_tag",
            '' as "l_subfield_cd",
            '' as "l_ind1_cd",
            '' as "l_ind2_cd",
            l_bibid as "l_field_data"
        from export_biblio_tsv
        where l_bibid = x_bibid
    ) x
) y;

END
$func$ LANGUAGE plpgsql;

-- add koha holding tag to marc
DROP FUNCTION IF EXISTS migration_tools.generate_koha_holding_tag(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION migration_tools.generate_koha_holding_tag(marc TEXT, tag TEXT, ind1 TEXT, ind2 TEXT, barcode TEXT, dateaccessioned TEXT, booksellerid TEXT, homebranch TEXT, price TEXT, replacementprice TEXT, replacementpricedate TEXT, datelastborrowed TEXT, datelastseen TEXT, stack TEXT, notforloan TEXT, damaged TEXT, itemlost TEXT, wthdrawn TEXT, itemcallnumber TEXT, issues TEXT, renewals TEXT, reserves TEXT, restricted TEXT, internalnotes TEXT, itemnotes TEXT, holdingbranch TEXT, location TEXT, onloan TEXT, cn_source TEXT, cn_sort TEXT, ccode TEXT, materials TEXT, uri TEXT, itype TEXT, enumchron TEXT, copynumber TEXT, stocknumber TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my ($marc_xml, $tag , $ind1 , $ind2 , $barcode , $dateaccessioned , $booksellerid , $homebranch , $price , $replacementprice , $replacementpricedate , $datelastborrowed , $datelastseen , $stack , $notforloan , $damaged , $itemlost , $wthdrawn , $itemcallnumber , $issues , $renewals , $reserves , $restricted , $internalnotes , $itemnotes , $holdingbranch , $location , $onloan , $cn_source , $cn_sort , $ccode , $materials , $uri , $itype , $enumchron , $copynumber , $stocknumber ) = @_;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my $new_field = new MARC::Field(
    $tag, $ind1, $ind2,
    'a' => $homebranch,
    'b' => $holdingbranch,
    'c' => $location,
    'p' => $barcode,
    'y' => $itype
);

if ($dateaccessioned) { $new_field->add_subfields('d' => $dateaccessioned); }
if ($booksellerid) { $new_field->add_subfields('e' => $booksellerid); }
if ($price) { $new_field->add_subfields('g' => $price); }
if ($replacementprice) { $new_field->add_subfields('v' => $replacementprice); }
if ($replacementpricedate) { $new_field->add_subfields('w' => $replacementpricedate); }
if ($datelastborrowed) { $new_field->add_subfields('s' => $datelastborrowed); }
if ($datelastseen) { $new_field->add_subfields('r' => $datelastseen); }
if ($stack) { $new_field->add_subfields('j' => $stack); }
if ($notforloan) { $new_field->add_subfields('7' => $notforloan); }
if ($damaged) { $new_field->add_subfields('4' => $damaged); }
if ($itemlost) { $new_field->add_subfields('1' => $itemlost); }
if ($wthdrawn) { $new_field->add_subfields('0' => $wthdrawn); }
if ($itemcallnumber) { $new_field->add_subfields('o' => $itemcallnumber); }
if ($issues) { $new_field->add_subfields('l' => $issues); }
if ($renewals) { $new_field->add_subfields('m' => $renewals); }
if ($reserves) { $new_field->add_subfields('n' => $reserves); }
if ($restricted) { $new_field->add_subfields('5' => $restricted); }
if ($internalnotes) { $new_field->add_subfields('x' => $internalnotes); }
if ($itemnotes) { $new_field->add_subfields('z' => $itemnotes); }
if ($onloan) { $new_field->add_subfields('q' => $onloan); }
if ($cn_source) { $new_field->add_subfields('2' => $cn_source); }
if ($cn_sort) { $new_field->add_subfields('6' => $cn_sort); }
if ($ccode) { $new_field->add_subfields('8' => $ccode); }
if ($materials) { $new_field->add_subfields('3' => $materials); }
if ($uri) { $new_field->add_subfields('u' => $uri); }
if ($enumchron) { $new_field->add_subfields('h' => $enumchron); }
if ($copynumber) { $new_field->add_subfields('t' => $copynumber); }
if ($stocknumber) { $new_field->add_subfields('i' => $stocknumber); }

$marc_xml->insert_grouped_field( $new_field );

return $marc_xml->as_xml_record();

$function$;

