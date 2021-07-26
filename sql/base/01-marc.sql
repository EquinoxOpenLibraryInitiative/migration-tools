DROP FUNCTION IF EXISTS migration_tools.strip_subfield(TEXT,CHAR(3),CHAR(1));
CREATE OR REPLACE FUNCTION migration_tools.strip_subfield(marc TEXT, tag CHAR(3), subfield CHAR(1))
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

my $marc_xml = shift;
my $tag = shift;
my $subfield = shift;
$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @fields = $marc_xml->field($tag);
return $marc_xml->as_xml_record() unless @fields;

$marc_xml->delete_fields(@fields);

foreach my $f (@fields) {
    $f->delete_subfield(code => $subfield);
}
$marc_xml->insert_fields_ordered(@fields);

return $marc_xml->as_xml_record();

$function$;


CREATE OR REPLACE FUNCTION migration_tools.set_leader (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $leader = $marc->leader();
    substr($leader, $pos, 1) = $value;
    $marc->leader($leader);
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.set_008 (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $f008 = $marc->field('008');

    if ($f008) {
       my $field = $f008->data();
       substr($field, $pos, 1) = $value;
       $f008->update($field);
       $xml = $marc->as_xml_record;
       $xml =~ s/^<\?.+?\?>$//mo;
       $xml =~ s/\n//sgo;
       $xml =~ s/>\s+</></sgo;
    }
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.insert_tags (TEXT, TEXT) RETURNS TEXT AS $$

  my ($marcxml, $tags) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $to_insert = MARC::Record->new_from_xml("<record>$tags</record>", 'UTF-8');

    my @incumbents = ();

    foreach my $field ( $marc->fields() ) {
      push @incumbents, $field->as_formatted();
    }

    foreach $field ( $to_insert->fields() ) {
      if (!grep {$_ eq $field->as_formatted()} @incumbents) {
        $marc->insert_fields_ordered( ($field) );
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.marc_parses( TEXT ) RETURNS BOOLEAN AS $func$

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $xml = shift;

eval {
    my $r = MARC::Record->new_from_xml( $xml );
    my $output_xml = $r->as_xml_record();
};
if ($@) {
    return 0;
} else {
    return 1;
}

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.marc_parses(TEXT) IS 'Return boolean indicating if MARCXML string is parseable by MARC::File::XML';

CREATE OR REPLACE FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

MARC::Charset->assume_unicode(1);

my $target_xml = shift;
my $source_xml = shift;
my $tags = shift;

my $target;
my $source;

eval { $target = MARC::Record->new_from_xml( $target_xml ); };
if ($@) {
    return;
}
eval { $source = MARC::Record->new_from_xml( $source_xml ); };
if ($@) {
    return;
}

my $source_id = $source->subfield('901', 'c');
$source_id = $source->subfield('903', 'a') unless $source_id;
my $target_id = $target->subfield('901', 'c');
$target_id = $target->subfield('903', 'a') unless $target_id;

my %existing_fields;
foreach my $tag (@$tags) {
    my %existing_fields = map { $_->as_formatted() => 1 } $target->field($tag);
    my @to_add = grep { not exists $existing_fields{$_->as_formatted()} } $source->field($tag);
    $target->insert_fields_ordered(map { $_->clone() } @to_add);
    if (@to_add) {
        elog(NOTICE, "Merged $tag tag(s) from $source_id to $target_id");
    }
}

my $xml = $target->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.merge_marc_fields( TEXT, TEXT, TEXT[] ) IS 'Given two MARCXML strings and an array of tags, returns MARCXML representing the merge of the specified fields from the second MARCXML record into the first.';

CREATE OR REPLACE FUNCTION migration_tools.make_stub_bib (text[], text[]) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use Text::CSV;

my $in_tags = shift;
my $in_values = shift;

# hack-and-slash parsing of array-passed-as-string;
# this can go away once everybody is running Postgres 9.1+
my $csv = Text::CSV->new({binary => 1});
$in_tags =~ s/^{//;
$in_tags =~ s/}$//;
my $status = $csv->parse($in_tags);
my $tags = [ $csv->fields() ];
$in_values =~ s/^{//;
$in_values =~ s/}$//;
$status = $csv->parse($in_values);
my $values = [ $csv->fields() ];

my $marc = MARC::Record->new();

$marc->leader('00000nam a22000007  4500');
$marc->append_fields(MARC::Field->new('008', '000000s                       000   eng d'));

foreach my $i (0..$#$tags) {
    my ($tag, $sf);
    if ($tags->[$i] =~ /^(\d{3})([0-9a-z])$/) {
        $tag = $1;
        $sf = $2;
        $marc->append_fields(MARC::Field->new($tag, ' ', ' ', $sf => $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    } elsif ($tags->[$i] =~ /^(\d{3})$/) {
        $tag = $1;
        $marc->append_fields(MARC::Field->new($tag, $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    }
}

my $xml = $marc->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.make_stub_bib (text[], text[]) IS $$Simple function to create a stub MARCXML bib from a set of columns.
The first argument is an array of tag/subfield specifiers, e.g., ARRAY['001', '245a', '500a'].
The second argument is an array of text containing the values to plug into each field.
If the value for a given field is NULL or the empty string, it is not inserted.
$$;

CREATE OR REPLACE FUNCTION migration_tools.make_stub_bib (text[], text[], text[], text[]) RETURNS TEXT AS $func$

use strict;
use warnings;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use Text::CSV;

my $in_tags = shift;
my $in_ind1 = shift;
my $in_ind2 = shift;
my $in_values = shift;

# hack-and-slash parsing of array-passed-as-string;
# this can go away once everybody is running Postgres 9.1+
my $csv = Text::CSV->new({binary => 1});
$in_tags =~ s/^{//;
$in_tags =~ s/}$//;
my $status = $csv->parse($in_tags);
my $tags = [ $csv->fields() ];
$in_ind1 =~ s/^{//;
$in_ind1 =~ s/}$//;
$status = $csv->parse($in_ind1);
my $ind1s = [ $csv->fields() ];
$in_ind2 =~ s/^{//;
$in_ind2 =~ s/}$//;
$status = $csv->parse($in_ind2);
my $ind2s = [ $csv->fields() ];
$in_values =~ s/^{//;
$in_values =~ s/}$//;
$status = $csv->parse($in_values);
my $values = [ $csv->fields() ];

my $marc = MARC::Record->new();

$marc->leader('00000nam a22000007  4500');
$marc->append_fields(MARC::Field->new('008', '000000s                       000   eng d'));

foreach my $i (0..$#$tags) {
    my ($tag, $sf);
    if ($tags->[$i] =~ /^(\d{3})([0-9a-z])$/) {
        $tag = $1;
        $sf = $2;
        $marc->append_fields(MARC::Field->new($tag, $ind1s->[$i], $ind2s->[$i], $sf => $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    } elsif ($tags->[$i] =~ /^(\d{3})$/) {
        $tag = $1;
        $marc->append_fields(MARC::Field->new($tag, $values->[$i])) if $values->[$i] !~ /^\s*$/ and $values->[$i] ne 'NULL';
    }
}

my $xml = $marc->as_xml_record;
$xml =~ s/^<\?.+?\?>$//mo;
$xml =~ s/\n//sgo;
$xml =~ s/>\s+</></sgo;

return $xml;

$func$ LANGUAGE PLPERLU;
COMMENT ON FUNCTION migration_tools.make_stub_bib (text[], text[], text[], text[]) IS $$Simple function to create a stub MARCXML bib from a set of columns.
The first argument is an array of tag/subfield specifiers, e.g., ARRAY['001', '245a', '500a'].
The second argument is an array of text containing the values to plug into indicator 1 for each field.
The third argument is an array of text containing the values to plug into indicator 2 for each field.
The fourth argument is an array of text containing the values to plug into each field.
If the value for a given field is NULL or the empty string, it is not inserted.
$$;

CREATE OR REPLACE FUNCTION migration_tools.set_indicator (TEXT, TEXT, INTEGER, CHAR(1)) RETURNS TEXT AS $func$

my ($marcxml, $tag, $pos, $value) = @_;

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use strict;

MARC::Charset->assume_unicode(1);

elog(ERROR, 'indicator position must be either 1 or 2') unless $pos =~ /^[12]$/;
elog(ERROR, 'MARC tag must be numeric') unless $tag =~ /^\d{3}$/;
elog(ERROR, 'MARC tag must not be control field') if $tag =~ /^00/;
elog(ERROR, 'Value must be exactly one character') unless $value =~ /^.$/;

my $xml = $marcxml;
eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');

    foreach my $field ($marc->field($tag)) {
        $field->update("ind$pos" => $value);
    }
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
};
return $xml;

$func$ LANGUAGE PLPERLU;

COMMENT ON FUNCTION migration_tools.set_indicator(TEXT, TEXT, INTEGER, CHAR(1)) IS $$Set indicator value of a specified MARC field.
The first argument is a MARCXML string.
The second argument is a MARC tag.
The third argument is the indicator position, either 1 or 2.
The fourth argument is the character to set the indicator value to.
All occurences of the specified field will be changed.
The function returns the revised MARCXML string.$$;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_leader (TEXT) RETURNS TEXT AS $$
    my ($marcxml) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->leader();
    };
    return $field;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tag (TEXT, TEXT) RETURNS TEXT AS $$
    my ($marcxml, $tag) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->field($tag);
    };
    return $field->as_string() if $field;
    return;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tag (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my $field;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        $field = $marc->field($tag);
    };
    return $field->as_string($subfield,$delimiter) if $field;
    return;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tags (TEXT, TEXT, TEXT, TEXT) RETURNS TEXT[] AS $$
    my ($marcxml, $tag, $subfield, $delimiter) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($tag);
    };
    my @texts;
    foreach my $field (@fields) {
        push @texts, $field->as_string($subfield,$delimiter);
    }
    return \@texts;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.get_marc_tags_filtered (TEXT, TEXT, TEXT, TEXT, TEXT) RETURNS TEXT[] AS $$
    my ($marcxml, $tag, $subfield, $delimiter, $match) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($tag);
    };
    my @texts;
    foreach my $field (@fields) {
        if ($field->as_string() =~ qr/$match/) {
            push @texts, $field->as_string($subfield,$delimiter);
        }
    }
    return \@texts;
$$ LANGUAGE PLPERLU STABLE;

DROP FUNCTION IF EXISTS migration_tools.merge_sf9(BIGINT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.merge_sf9(bib_id BIGINT,new_sf9 TEXT,force TEXT DEFAULT 'false')
    RETURNS BOOLEAN AS
$BODY$
DECLARE
    marc_xml    TEXT;
    new_marc    TEXT;
BEGIN
    SELECT marc FROM biblio.record_entry WHERE id = bib_id INTO marc_xml;

    SELECT munge_sf9(marc_xml,new_sf9,force) INTO new_marc;
    UPDATE biblio.record_entry SET marc = new_marc WHERE id = bib_id;

    RETURN true;
END;
$BODY$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS migration_tools.munge_sf9(TEXT,TEXT,TEXT);
DROP FUNCTION IF EXISTS migration_tools.munge_sf9_qualifying_match(TEXT,TEXT,TEXT,TEXT);
-- removing the depredated munge_sf9 and deprecsated version of munge_sf_qualifying_match 

DROP FUNCTION IF EXISTS migration_trools.munge_sf9_qualifying_match(TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.munge_sf9_qualifying_match(marc_xml text, qualifying_match text, new_9_to_set text)
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

my $marc_xml = shift;
my $qualifying_match = shift;
my $new_9_to_set = shift;
my $force = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

$qualifying_match = lc($qualifying_match);
foreach my $field (@uris) {
    my $str = lc($field->as_string());
    if (index($str, $qualifying_match) != -1 or $qualifying_match eq '*') { #checks whole tag not just $u
        elog(NOTICE, "test passes for $str\n");
        my $ind1 = $field->indicator('1');
        my $ind2 = $field->indicator('2');
        if (!defined $ind1) { $field->set_indicator(1,'4'); }
        if (!defined $ind2) { $field->set_indicator(2,'0'); }
        if ($ind1 ne '1' && $ind1 ne '4') { $field->set_indicator(1,'4'); }
        if ($ind2 ne '0' && $ind2 ne '1') { $field->set_indicator(2,'0'); }
        $field->add_subfields( '9' => $new_9_to_set );
    }
}

return $marc_xml->as_xml_record();

$function$



DROP FUNCTION IF EXISTS migration_tools.remove_sf9(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.remove_sf9(marc_xml TEXT, nine_to_del TEXT)
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict; 
use warnings;

use MARC::Record;
use MARC::Field;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $nine_to_del = shift;
$nine_to_del =~ s/^\s+|\s+$//g;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    $field->delete_subfield(code => '9', match => qr/$nine_to_del/);
}

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.additional_sf9_qualifying_match(TEXT,TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.additional_sf9_qualifying_match(marc_xml TEXT, qualifying_match TEXT, new_9_to_set TEXT, qualifying_sf9 TEXT, force TEXT DEFAULT 'true')
 RETURNS TEXT
 LANGUAGE plperlu
AS $function$
use strict;
use warnings;
use MARC::Record;
use MARC::Field;
use MARC::File::XML (BinaryEncoding => 'utf8');

binmode(STDERR, ':bytes');
binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $marc_xml = shift;
my $qualifying_match = shift;
my $new_9_to_set = shift;
my $qualifying_sf9 = shift;
my $force = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;
eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;
$qualifying_match =~ s/^\s+|\s+$//g;
$qualifying_sf9 =~ s/^\s+|\s+$//g;
$qualifying_sf9 = lc($qualifying_sf9);

foreach my $field (@uris) {
    my @us = $field->subfield('u');
    my @nines = $field->subfield('9');
    my $has_u;
    my $has_9;
    foreach my $u (@us) {
        if ($u =~ qr/$qualifying_match/) { $has_u = 1; }
    }
    foreach my $nine (@nines) {
        $nine =~ s/^\s+|\s+$//g;
        if (lc($nine) eq $qualifying_sf9) { $has_9 = 1; }
    }
    if ($has_u and $has_9) {
        my $ind1 = $field->indicator('1');
        if (!defined $ind1) { next; }
        if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'false') { next; }
        if ($ind1 ne '1' && $ind1 ne '4' && $force eq 'true') { $field->set_indicator(1,'4'); }
        my $ind2 = $field->indicator('2');
        if (!defined $ind2) { next; }
        if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'false') { next; }
        if ($ind2 ne '0' && $ind2 ne '1' && $force eq 'true') { $field->set_indicator(2,'0'); }
        $field->add_subfields( '9' => $new_9_to_set );
    }
}
return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.owner_change_sf9_substring_match(TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.owner_change_sf9_substring_match (marc_xml TEXT, substring_old_value TEXT, new_value TEXT, fix_indicators TEXT)
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

my $marc_xml = shift;
my $substring_old_value = shift;
my $new_value = shift;
my $fix_indicators = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @uris = $marc_xml->field('856');
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    my $ind1 = $field->indicator('1');
    if (defined $ind1) {
        if ($ind1 ne '1' && $ind1 ne '4' && $fix_indicators eq 'true') {
            $field->set_indicator(1,'4');
        }
    }
    my $ind2 = $field->indicator('2');
    if (defined $ind2) {
        if ($ind2 ne '0' && $ind2 ne '1' && $fix_indicators eq 'true') {
            $field->set_indicator(2,'0');
        }
    }
    if ($field->as_string('9') =~ qr/$substring_old_value/) {
        $field->delete_subfield('9');
        $field->add_subfields( '9' => $new_value );
    }
    $marc_xml->delete_field($field); # -- we're going to dedup and add them back
}

my %hash = (map { ($_->as_usmarc => $_) } @uris); # -- courtesy of an old Mike Rylander post :-)
$marc_xml->insert_fields_ordered( values( %hash ) );

return $marc_xml->as_xml_record();

$function$;

DROP FUNCTION IF EXISTS migration_tools.owner_change_sf9_substring_match2(TEXT,TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.owner_change_sf9_substring_match2 (marc_xml TEXT, qualifying_match TEXT, substring_old_value TEXT, new_value TEXT, fix_indicators TEXT)
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

my $marc_xml = shift;
my $qualifying_match = shift;
my $substring_old_value = shift;
my $new_value = shift;
my $fix_indicators = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @unqualified_uris = $marc_xml->field('856');
my @uris = ();
foreach my $field (@unqualified_uris) {
    if ($field->as_string() =~ qr/$qualifying_match/) {
        push @uris, $field;
    }
}
return $marc_xml->as_xml_record() unless @uris;

foreach my $field (@uris) {
    my $ind1 = $field->indicator('1');
    if (defined $ind1) {
        if ($ind1 ne '1' && $ind1 ne '4' && $fix_indicators eq 'true') {
            $field->set_indicator(1,'4');
        }
    }
    my $ind2 = $field->indicator('2');
    if (defined $ind2) {
        if ($ind2 ne '0' && $ind2 ne '1' && $fix_indicators eq 'true') {
            $field->set_indicator(2,'0');
        }
    }
    if ($field->as_string('9') =~ qr/$substring_old_value/) {
        $field->delete_subfield('9');
        $field->add_subfields( '9' => $new_value );
    }
    $marc_xml->delete_field($field); # -- we're going to dedup and add them back
}

my %hash = (map { ($_->as_usmarc => $_) } @uris); # -- courtesy of an old Mike Rylander post :-)
$marc_xml->insert_fields_ordered( values( %hash ) );

return $marc_xml->as_xml_record();

$function$;

-- strip marc tag
DROP FUNCTION IF EXISTS migration_tools.strip_tag(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.strip_tag(marc TEXT, tag TEXT)
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

my $marc_xml = shift;
my $tag = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @fields = $marc_xml->field($tag);
return $marc_xml->as_xml_record() unless @fields;

$marc_xml->delete_fields(@fields);

return $marc_xml->as_xml_record();

$function$;

-- removes tags from record based on tag, subfield and evidence
-- example: strip_tag(marc, '500', 'a', 'gift') will remove 500s with 'gift' as a part of the $a
DROP FUNCTION IF EXISTS migration_tools.strip_tag(TEXT,TEXT,TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.strip_tag(marc TEXT, tag TEXT, subfield TEXT, evidence TEXT)
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

my $marc_xml = shift;
my $tag = shift;
my $subfield = shift;
my $evidence = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @fields = $marc_xml->field($tag);
return $marc_xml->as_xml_record() unless @fields;

my @fields_to_delete;

foreach my $f (@fields) {
    my $sf = lc($f->as_string($subfield));
    if ($sf =~ m/$evidence/) { push @fields_to_delete, $f; }
}

$marc_xml->delete_fields(@fields_to_delete);

return $marc_xml->as_xml_record();

$function$;

-- consolidate marc tag
DROP FUNCTION IF EXISTS migration_tools.consolidate_tag(TEXT,TEXT);
CREATE OR REPLACE FUNCTION migration_tools.consolidate_tag(marc TEXT, tag TEXT)
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

my $marc_xml = shift;
my $tag = shift;

$marc_xml =~ s/(<leader>.........)./${1}a/;

eval {
    $marc_xml = MARC::Record->new_from_xml($marc_xml);
};
if ($@) {
    #elog("could not parse $bibid: $@\n");
    import MARC::File::XML (BinaryEncoding => 'utf8');
    return $marc_xml;
}

my @fields = $marc_xml->field($tag);
return $marc_xml->as_xml_record() unless @fields;

my @combined_subfield_refs = ();
my @combined_subfields = ();
foreach my $field (@fields) {
    my @subfield_refs = $field->subfields();
    push @combined_subfield_refs, @subfield_refs;
}

my @sorted_subfield_refs = reverse sort { $a->[0] <=> $b->[0] } @combined_subfield_refs;

while ( my $tuple = pop( @sorted_subfield_refs ) ) {
    my ($code,$data) = @$tuple;
    unshift( @combined_subfields, $code, $data );
}

$marc_xml->delete_fields(@fields);

my $new_field = new MARC::Field(
    $tag,
    $fields[0]->indicator(1),
    $fields[0]->indicator(2),
    @combined_subfields
);

$marc_xml->insert_grouped_field( $new_field );

return $marc_xml->as_xml_record();

$function$;

CREATE OR REPLACE FUNCTION migration_tools.set_leader (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $leader = $marc->leader();
    substr($leader, $pos, 1) = $value;
    $marc->leader($leader);
    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.set_008 (TEXT, INT, TEXT) RETURNS TEXT AS $$
  my ($marcxml, $pos, $value) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;
  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $f008 = $marc->field('008');

    if ($f008) {
       my $field = $f008->data();
       substr($field, $pos, 1) = $value;
       $f008->update($field);
       $xml = $marc->as_xml_record;
       $xml =~ s/^<\?.+?\?>$//mo;
       $xml =~ s/\n//sgo;
       $xml =~ s/>\s+</></sgo;
    }
  };
  return $xml;
$$ LANGUAGE PLPERLU STABLE;

CREATE OR REPLACE FUNCTION migration_tools.insert_tags (TEXT, TEXT) RETURNS TEXT AS $$

  my ($marcxml, $tags) = @_;

  use MARC::Record;
  use MARC::File::XML;

  my $xml = $marcxml;

  eval {
    my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
    my $to_insert = MARC::Record->new_from_xml("<record>$tags</record>", 'UTF-8');

    my @incumbents = ();

    foreach my $field ( $marc->fields() ) {
      push @incumbents, $field->as_formatted();
    }

    foreach $field ( $to_insert->fields() ) {
      if (!grep {$_ eq $field->as_formatted()} @incumbents) {
        $marc->insert_fields_ordered( ($field) );
      }
    }

    $xml = $marc->as_xml_record;
    $xml =~ s/^<\?.+?\?>$//mo;
    $xml =~ s/\n//sgo;
    $xml =~ s/>\s+</></sgo;
  };

  return $xml;

$$ LANGUAGE PLPERLU STABLE;


CREATE OR REPLACE FUNCTION migration_tools.marc_set_tag (TEXT, TEXT, TEXT) RETURNS TEXT AS $$
    my ($marcxml, $source_tag, $new_tag) = @_;

    use MARC::Record;
    use MARC::File::XML;
    use MARC::Field;

    my @fields;
    eval {
        my $marc = MARC::Record->new_from_xml($marcxml, 'UTF-8');
        @fields = $marc->field($source_tag);
    };
    foreach my $field (@fields) {
        $field->set_tag($new_tag);
    }
    return $marc->as_xml_record();
$$ LANGUAGE PLPERLU STABLE;
