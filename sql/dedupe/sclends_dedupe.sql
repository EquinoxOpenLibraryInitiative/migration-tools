-- SCLENDS bibliographic dedupe routine
--
-- Copyright 2010-2011 Equinox Software, Inc.
-- Author: Galen Charlton
--
-- This implements a bibliographic deduplication routine based
-- on criteria and an algorithm specified by the South Carolina
-- State Library on behalf of the SC LENDS consortium.  This work
-- was sponsored by SC LENDS, whose impetus is gratefully
-- acknowledged.  Portions of this script were subseqently expanded
-- based on the advice of the Indiana State Library on the behalf
-- of the Evergreen Indiana project.

-- schema to store the dedupe routine and intermediate data
CREATE SCHEMA m_dedupe;

CREATE TYPE mig_isbn_match AS (norm_isbn TEXT, norm_title TEXT, qual TEXT, bibid BIGINT);

-- function to calculate the normalized ISBN and title match keys
-- and the bibliographic portion of the quality score.  The normalized
-- ISBN key consists of the set of 020$a and 020$z normalized as follows:
--  * numeric portion of the ISBN converted to ISBN-13 format
--
-- The normalized title key is taken FROM the 245$a with the nonfiling
-- characters and leading and trailing whitespace removed, ampersands
-- converted to ' and ', other punctuation removed, and the text converted
-- to lowercase.
--
-- The quality score is a 19-digit integer computed by concatenating
-- counts of various attributes in the MARC records; see the get_quality
-- routine for details.
--
CREATE OR REPLACE FUNCTION m_dedupe.get_isbn_match_key (bib_id BIGINT, marc TEXT) RETURNS SETOF mig_isbn_match AS $func$
use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');
use Business::ISBN;

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $get_quality = sub {
    my $marc = shift;

    my $has003 = (scalar($marc->field('003'))) ? '1' : '0';

    return join('', $has003,
                    count_field($marc, '02.'),
                    count_field($marc, '24.'),
                    field_length($marc, '300'),               
                    field_length($marc, '100'),               
                    count_field($marc, '010'),
                    count_field($marc, '50.', '51.', '52.', '53.', '54.', '55.', '56.', '57.', '58.'),
                    count_field($marc, '6..'),
                    count_field($marc, '440', '490', '830'),
                    count_field($marc, '7..'),
                );
};

my ($bibid, $xml) = @_;

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

my @f245 = $marc->field('245');
return unless @f245; # must have 245
my $norm_title = norm_title($f245[0]);
return unless $norm_title ne '';

my @isbns = $marc->field('020');
return unless @isbns; # must have at least 020

my $qual = $get_quality->($marc);

my @norm_isbns = norm_isbns(@isbns);
foreach my $isbn (@norm_isbns) {
    return_next({ norm_isbn => $isbn, norm_title => $norm_title, qual => $qual, bibid => $bibid });
}
return undef;


sub count_field {
    my ($marc) = shift;
    my @tags = @_;
    my $total = 0;
    foreach my $tag (@tags) {
        my @f = $marc->field($tag);
        $total += scalar(@f);
    }
    $total = 99 if $total > 99;
    return sprintf("%-02.2d", $total);
}

sub field_length {
    my $marc = shift;
    my $tag = shift;

    my @f = $marc->field($tag);
    return '00' unless @f;
    my $len = length($f[0]->as_string);
    $len = 99 if $len > 99;
    return sprintf("%-02.2d", $len);
}

sub norm_title {
    my $f245 = shift;
    my $sfa = $f245->subfield('a');
    return '' unless defined $sfa;
    my $nonf = $f245->indicator(2);
    $nonf = '0' unless $nonf =~ /^\d$/;
    if ($nonf == 0) {
        $sfa =~ s/^a //i;
        $sfa =~ s/^an //i;
        $sfa =~ s/^the //i;
    } else {
        $sfa = substr($sfa, $nonf);
    }
    $sfa =~ s/&/ and /g;
    $sfa = lc $sfa;
    $sfa =~ s/\[large print\]//;
    $sfa =~ s/[[:punct:]]//g;
    $sfa =~ s/^\s+//;
    $sfa =~ s/\s+$//;
    $sfa =~ s/\s+/ /g;
    return $sfa;
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

sub norm_isbn {
    my $str = shift;
    my $norm = '';
    return '' unless defined $str;
    $str =~ s/-//g;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
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
$func$ LANGUAGE PLPERLU;

-- Specify set of bibs to dedupe.  This version
-- simply collects the IDs of all non-deleted bibs,
-- but the query could be expanded to exclude bibliographic
-- records that should not participate in the deduplication.
CREATE TABLE m_dedupe.bibs_to_check AS
SELECT id AS bib_id 
FROM biblio.record_entry bre
WHERE NOT deleted;

-- staging table for the match keys
CREATE TABLE m_dedupe.match_keys (
  norm_isbn TEXT,
  norm_title TEXT,
  qual TEXT,
  bibid BIGINT
);

-- calculate match keys
INSERT INTO m_dedupe.match_keys 
SELECT  (a.get_isbn_match_key::mig_isbn_match).norm_isbn,
        (a.get_isbn_match_key::mig_isbn_match).norm_title,
        (a.get_isbn_match_key::mig_isbn_match).qual,
        (a.get_isbn_match_key::mig_isbn_match).bibid                                                                                 
FROM (
    SELECT m_dedupe.get_isbn_match_key(bre.id, bre.marc)
    FROM biblio.record_entry bre
    JOIN m_dedupe.bibs_to_check c ON (c.bib_id = bre.id)
) a;

CREATE INDEX norm_idx on m_dedupe.match_keys(norm_isbn, norm_title);
CREATE INDEX qual_idx on m_dedupe.match_keys(qual);

-- and remove duplicates
CREATE TEMPORARY TABLE uniq_match_keys AS 
SELECT DISTINCT norm_isbn, norm_title, qual, bibid
FROM m_dedupe.match_keys;

DELETE FROM m_dedupe.match_keys;
INSERT INTO m_dedupe.match_keys SELECT * FROM uniq_match_keys;

-- find highest-quality match keys
CREATE TABLE m_dedupe.lead_quals AS
SELECT max(qual) as max_qual, norm_isbn, norm_title
FROM m_dedupe.match_keys
GROUP BY norm_isbn, norm_title
HAVING COUNT(*) > 1;

CREATE INDEX norm_idx2 ON m_dedupe.lead_quals(norm_isbn, norm_title);
CREATE INDEX norm_qual_idx2 ON m_dedupe.lead_quals(norm_isbn, norm_title, max_qual);

-- identify prospective lead bibs
CREATE TABLE m_dedupe.prospective_leads AS
SELECT bibid, a.norm_isbn, a.norm_title, b.max_qual, count(ac.id) as copy_count
FROM m_dedupe.match_keys a
JOIN m_dedupe.lead_quals b on (a.qual = b.max_qual and a.norm_isbn = b.norm_isbn and a.norm_title = b.norm_title)
JOIN asset.call_number acn on (acn.record = bibid)
JOIN asset.copy ac on (ac.call_number = acn.id)
WHERE not acn.deleted
and not ac.deleted
GROUP BY bibid, a.norm_isbn, a.norm_title, b.max_qual;

-- and use number of copies to break ties
CREATE TABLE m_dedupe.best_lead_keys AS
SELECT norm_isbn, norm_title, max_qual, max(copy_count) AS copy_count
FROM m_dedupe.prospective_leads
GROUP BY norm_isbn, norm_title, max_qual;

CREATE TABLE m_dedupe.best_leads AS
SELECT bibid, a.norm_isbn, a.norm_title, a.max_qual, copy_count
FROM m_dedupe.best_lead_keys a
JOIN m_dedupe.prospective_leads b USING (norm_isbn, norm_title, max_qual, copy_count);

-- and break any remaining ties using the lowest bib ID as the winner
CREATE TABLE m_dedupe.unique_leads AS
SELECT MIN(bibid) AS lead_bibid, norm_isbn, norm_title, max_qual
FROM m_dedupe.best_leads
GROUP BY norm_isbn, norm_title, max_qual;

-- start computing the merge map
CREATE TABLE m_dedupe.merge_map_pre
AS SELECT distinct lead_bibid, bibid as sub_bibid 
FROM m_dedupe.unique_leads
JOIN m_dedupe.match_keys using (norm_isbn, norm_title)
WHERE lead_bibid <> bibid;

-- and resolve transitive maps
UPDATE m_dedupe.merge_map_pre a
SET lead_bibid = b.lead_bibid
FROM m_dedupe.merge_map_pre b
WHERE a.lead_bibid = b.sub_bibid;

UPDATE m_dedupe.merge_map_pre a
SET lead_bibid = b.lead_bibid
FROM m_dedupe.merge_map_pre b
WHERE a.lead_bibid = b.sub_bibid;

UPDATE m_dedupe.merge_map_pre a
SET lead_bibid = b.lead_bibid
FROM m_dedupe.merge_map_pre b
WHERE a.lead_bibid = b.sub_bibid;

-- and produce the final merge map
CREATE TABLE m_dedupe.merge_map
AS SELECT min(lead_bibid) as lead_bibid, sub_bibid
FROM m_dedupe.merge_map_pre
GROUP BY sub_bibid;

-- add a unique ID to the merge map so that
-- we can do the actual record merging in chunks
ALTER TABLE m_dedupe.merge_map ADD COLUMN id serial, ADD COLUMN done BOOLEAN DEFAULT FALSE;

-- and here's an example of processing a chunk of a 1000
-- merges
SELECT asset.merge_record_assets(lead_bibid, sub_bibid)
FROM m_dedupe.merge_map WHERE id in (
  SELECT id FROM m_dedupe.merge_map
  WHERE done = false
  ORDER BY id
  LIMIT 1000
);

UPDATE m_dedupe.merge_map set done = true
WHERE id in (
  SELECT id FROM m_dedupe.merge_map
  WHERE done = false
  ORDER BY id
  LIMIT 1000
);

