#!/usr/bin/perl

#version .2 alvt migration 
#version .3 visual art migration 
#version .4 weeksville

use strict;
use warnings;
use MARC::Record;
use MARC::Batch;
use MARC::Field;
use Text::CSV_XS;
use Data::Dumper;
use List::MoreUtils qw(first_index);
use List::MoreUtils qw(uniq);
use Getopt::Long;

no warnings 'experimental::smartmatch';

my $marc_out;
my $csv_in;
my $delimiter = ',';
my $config;
my $limit;
my $progress;
my $unnest_limit = 3000;
my $qc = '"';
my $def_942;
my $def_rec_type;

my $ret = GetOptions(
    'delimiter:s'    => \$delimiter,
    'marc_out:s'     => \$marc_out,
    'config:s'       => \$config,
    'limit:i'        => \$limit,
    'progress:i'     => \$progress,
    'unnest_limit:i' => \$unnest_limit,
    'quote_char:s'   => \$qc,
    'def_942:s'      => \$def_942,
    'def_rec_type:s' => \$def_rec_type,
    'csv_in:s'       => \$csv_in    
);

if (!defined $marc_out) { abort('no --marc_out provided'); }
if (!defined $csv_in)   { abort('no --csv_in provided'); }
if (!defined $config)   { abort('no --config provided'); }

my $csv = Text::CSV_XS->new({ binary => 1, sep_char => $delimiter, quote_char => $qc, allow_loose_quotes => 1 });
my $config_csv = Text::CSV_XS->new({ binary => 1, sep_char => ',' });
open my $marc_fh, '>:utf8', $marc_out or die "Can not open output file $!\n";
open my $in_fh, '<:utf8', $csv_in or die "Can not open base input file $!\n";
open my $config_fh, '<:utf8', $config or die "Can not open mapping file $!\n";

my @csv_mapping;
my $increment = 0;
while (my $line = <$config_fh>) {
   chomp $line;
   my $ind = index $line, '#';
   if ($ind == 0) { next; } #line is commented out
   $line =~ s/^\s+|\s+$//g;
   my @line_array = split /("[^"]*")/, $line;
   for (@line_array) {
       unless (/^"/) { s/[ \t]+//g; }
   }
   my $new_line = join '',@line_array;
   my @fields;
   if ($config_csv->parse($new_line)) {
       @fields = $config_csv->fields();
   } else {
       abort('mapping file failed to parse');
   }
   push @csv_mapping, \@fields;
   $increment++;
}

# this will hold positions in the csv file 
my %csv_header_positions;
foreach my $cm (@csv_mapping) {
    my $header = @$cm[0];
    if ($header) { $csv_header_positions{$header} = -1; } 
}

my $column_count = scalar @csv_mapping;
my $i = 0;
my %column_indexes;
my $bib_type_position;
while (my $line = <$in_fh>) {
    $i++;
    if ($progress) {
        if (($i % $progress) == 0) { print "reading line $i\n"; }
    }
    chomp $line;
    my $outrec;
    my $outrec_length;
    my $record;
    my $bibtype;
	if ($csv->parse($line)) {
		my @fields = $csv->fields();
		if ($i == 1) {  
           foreach my $key (keys %csv_header_positions) {
               $csv_header_positions{$key} = first_index { lc($_) =~ lc($key) } @fields;
           }
           for my $m (@csv_mapping) { 
               my $m_header = @$m[0];
               my $m_type = @$m[1];
               if ( $m_type eq 't' ) { $bib_type_position =  $csv_header_positions{$m_header}; last; }
           } 
		} else { 
            #get the bibtype
            if ($bib_type_position) { $bibtype = $fields[$bib_type_position]; }
            if (!defined $bib_type_position and $def_rec_type) 
                { $bibtype = $def_rec_type; }
		    $record = create_core_bib($bibtype);
            #make tags 
            $record = add_fields($record,\@csv_mapping,\@fields,\%csv_header_positions,$def_942);
            #update record length and get it again just in case it's changed 
            $outrec = $record->as_usmarc();
            $outrec_length = length $outrec;
            $outrec_length = sprintf("%05s",$outrec_length);
            my $ldr = $record->leader();
            substr($ldr,0,5) = $outrec_length;
            $record->leader($ldr);
            print $marc_fh $record->as_usmarc(),"\n"; 
		}
	} else {
            #check segments, if it failed because it is short maybe there is something we can do 
		    my $str = substr($line,0,40); 
            my $error = $csv->error_diag();
		    print "Line $i could not be parsed. $error\n"; 
            print Dumper $line;
	}
    if ($limit and $limit + 1 == $i) { print "limit set at $limit rows\n"; last; } 
}

close $marc_fh;
close $csv_in;

##############################################  beyond here be functions
########################################################################

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    exit 1;
}

sub add_fields {
    my $record = shift;
    my $mapping = shift;
    my $fields = shift;
    my $positions = shift;
    my $def_942 = shift;
    my @group_tags;

    #add 942 if it's defined 
    my $field_942;
    if ($def_942) {
        $field_942 = MARC::Field->new( '942', ' ', ' ', 'c' => $def_942 );
        $record->insert_fields_ordered($field_942);
    }

    #loop through the bib mappings, if there is no 8 group field create the field
    #if there is then do ... something  
    for my $m (@$mapping) {
        my $group_tag = @$m[8];
        if ( @$m[1] eq 'b' and $group_tag eq '') {
            $record = add_single_field($record,$m,$fields,$positions);
        }
        if ( @$m[1] eq 'b' and $group_tag ne '') { 
            push @group_tags, $group_tag;
        }
    } 

    my @unique_tags = uniq @group_tags;
    my $ugLength = scalar @unique_tags;
    my $i = 0;

    #should be able to use a single add_foo_field function by making the single field call use a list of list 
    #but for now this is easier for troubleshooting and wondering if grouped fields may want some funky logic 
    #in the future we don't want for single fields to have to go through 
    while ($i < $ugLength) {
        $i++;
        my $group = shift @unique_tags;
        my @group_mappings;
        for my $m (@$mapping) {
            if ( @$m[1] eq 'b' and @$m[8] eq $group) {
                push @group_mappings, $m;
            }
        }
        if (@group_mappings) { $record = add_grouped_field($record,\@group_mappings,$fields,$positions); }  
    }

    return $record;
}

sub add_grouped_field {
    my $record = shift;
    my $mapping = shift;  #list of lists 
    my $fields = shift; #list 
    my $positions = shift; #hash

    my $field;  
    foreach my $map (@$mapping) {
        my $tag = @$map[2];
        my $subfield = @$map[3];
        my $ind1 = @$map[4];
        my $ind2 = @$map[5];
        my $static_value = @$map[7];
        my $suffix = @$map[9];
        my $prefix = @$map[10];
        my $value_position = %$positions{@$map[0]};
        my $value = @$fields[$value_position];
        $value = static_test($value,$static_value,$value_position);
        $value = prep_value($value,$prefix,$suffix);
        if ($value and $field) { 
            $field->add_subfields( $subfield, $value ); 
        } 
        if ($value and !defined $field) {
            $field = MARC::Field->new( $tag, $ind1, $ind2, $subfield => $value);
        }
    }
    if ($field) { $record->insert_fields_ordered($field); }
    return $record;
}

sub add_single_field {
    my $record = shift;
    my $mapping = shift;  #list 
    my $fields = shift; #list 
    my $positions = shift; #hash

    my $field;
    my @valueList;
    my $tag = @$mapping[2];
    my $subfield = @$mapping[3];
    my $ind1 = @$mapping[4];
    my $ind2 = @$mapping[5];
    my $internal_delimiter = @$mapping[6];
    my $static_value = @$mapping[7];
    my $suffix = @$mapping[9];
    my $prefix = @$mapping[10];
    my $value_position = %$positions{@$mapping[0]};
    my $value = @$fields[$value_position];
    if ($internal_delimiter eq '') { undef $internal_delimiter; }
    $value = static_test($value,$static_value,$value_position);
    $value = prep_value($value,$prefix,$suffix);
    if ($value and !defined $internal_delimiter) {
        $field = MARC::Field->new( $tag, $ind1, $ind2, $subfield => $value);
        if ($field) { $record->insert_fields_ordered($field); }
    }
    my $lc = 0;
    if ($value and $internal_delimiter) {
        @valueList = split /$internal_delimiter/, $value;
        foreach ( @valueList ) { 
            $lc++;
            $field = MARC::Field->new( $tag, $ind1, $ind2, $subfield => $_);
            if ($field) { $record->insert_fields_ordered($field); } 
            if ($lc >= $unnest_limit) { last; }
        }
    }
    return $record;
}

sub create_core_bib {
	my $bibtype = shift;
    chomp $bibtype;
    my $record = MARC::Record->new();
    $record->encoding( 'UTF-8' );
    my %leaders = (
        '3-4'         => '00000ng  a22002057a 4500',
        audiobook     => '00000ni  a22002057a 4500',
        book          => '00000nam a22002057a 4500',
        dvd           => '00000ng  a22002057a 4500',
        equipment     => '00000nr  a22002057a 4500',
        kit           => '00000no  a22002057a 4500',
        mp4           => '00000ng  a22002057a 4500',
        serial        => '00000n s a22002057a 4500',
        '16mm'        => '00000ng  a22002057a 4500',
        vhs           => '00000ng  a22002057a 4500'
    );

    $record->leader($leaders{$bibtype});

	# boilerplate 007s
	my $new_bond;
    my %bonds = (
        '3-4'        => 'vd\|oafr|',
        audiobook    => 'sd\fungnn|||ed',
        dvd          => 'vd\|vairu',
        mp4          => 'vz\|zazz|',
        '16mm'       => 'vr\|z||z|',
        vhs          => 'vc\|bafo|'
    );

    if ( $bibtype ~~ [ '3-4', 'audiobook', 'dvd', 'mp4', '16mm', 'vhs' ] ) 
        { $new_bond = MARC::Field->new('007',$bonds{$bibtype}); }
	if (defined $new_bond) { $record->append_fields($new_bond); }

    # minimal 008
    my $date = create_date();
    my $zze    = "$date||||||||||||||||| |||||00| ||||||u";
    my $r_zze = MARC::Field->new('008',$zze); 
	$record->append_fields($r_zze); 

    # add a 005?  meh, there's the info in the 008

    return $record;
}

sub create_date {
    my @abbr = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    $year += 1900;
	$mon = sprintf("%02s",$mon);
	$mday = sprintf("%02s",$mday);
	$year = substr($year,-2);
    my $date = $year . $mon . $mday;
    return $date;
}

sub prep_value {
    my $str = shift;
    my $prefix = shift;
    my $suffix = shift;

    $str =~ s/\R//g;
    $str =~ s/^\s+|\s+$//g;
    if (length($str) < 1) { undef $str; }

    if ($suffix) {
        $suffix =~ s/\R//g;
        if (length($suffix) < 1) { undef $suffix; }
    }
    if ($str and $suffix) { $str = $str . $suffix; }

    if ($prefix) {
        $prefix =~ s/\R//g;
        if (length($prefix) < 1) { undef $prefix; }
    }
    if ($str and $prefix) { $str = $prefix . $str; }

    return $str;
}

sub static_test {
    my $value = shift;
    my $static_value = shift;
    my $value_position = shift;

    if ($value_position < 0) { $value = $static_value; }
    return $value;
}

sub trim {
	my $str = shift;
	$str =~ s/^\s+|\s+$//g;
	return $str;
}
