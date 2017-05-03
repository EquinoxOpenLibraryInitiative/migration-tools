#!/usr/bin/perl -w
# ./filter_out_mfhd.pl marcfile > out 2> err
# Looks for tcn_id.map2 containg lines like:  001_or_035value|eg_bib_id
# Spits out mfhd.tsv (eg_bib_id<tab>marcxml<tab>eg_bib_id) and mfhd.bad.mrc
# For marcfile, it expects a "title record", followed by one or more MFHD records.  Rinse, repeat.

use strict;
use warnings;
use open ':utf8';

use MARC::Batch;
use Unicode::Normalize;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;

my $batch = MARC::Batch->new( 'USMARC', @ARGV );
$batch->strict_off();
$batch->warnings_off();

my $current_title;
my $tag001;
my $tag035;
my $tag245;
my $tag852;
my $tag866;
my %tcn2bid;

open FILE, "tcn_id.map2";
while (my $line = <FILE>) {
    if ($line =~ /^(.+)\|(.*)$/) {
        $tcn2bid{$1} = $2;
    }
}
close FILE;

open MFHD, ">mfhd.tsv";
open BADMFHD, ">mfhd.bad.mrc";
while ( my $marc = $batch->next ) {
    $tag001 = $marc->field('001');
    $tag035 = $marc->field('035');
    $tag245 = $marc->field('245');
    $tag852 = $marc->field('852');
    $tag866 = $marc->field('866');
    if ($tag852 || $tag866) {
        print "\tMFHD\n";
        my $field = MARC::Field->new(
            '004',
            $tcn2bid{$current_title}
            ? $tcn2bid{$current_title}
            : 'missing: ' . $current_title
        );
        $marc->insert_fields_ordered( $field );
        if ($tcn2bid{$current_title}) {
            my $string = $marc->as_xml_record();
            $string =~ s/\n//g;
            $string =~ s/<\?xml version="1\.0" encoding="UTF-8"\?>//;
            print MFHD $tcn2bid{$current_title} . "\t$string\t" . $tcn2bid{$current_title} . "\n";
        } else {
            print BADMFHD $marc->as_usmarc();
        }
    } else {
        if ($tag001) {
            my $tcnv = $tag001->as_string();
            if ($tcnv =~ /^\d*$/) {
                print "fishy MFHD? with 001 $tcnv\n";
                print STDERR "=== fishy MFHD? with 001 $tcnv\n";
                print STDERR $marc->as_formatted() . "\n";
            } else {
                print "title with 001 $tcnv, eg bib id = $tcn2bid{$tcnv}\n";
                $current_title = $tcnv;
            }
        } else {
            if ($tag035) {
                my $tcnv = $tag035->as_string();
                print "title with 035 $tcnv, eg bib id = $tcn2bid{$tcnv}\n";
                $current_title = $tcnv;
            } else {
                my $tcnv;
                if ($tag245) {
                    $tcnv = $tag245->as_string();
                }
                print "fishy title? missing 001 and 035: $tcnv\n";
                print STDERR "=== fishy title? missing 001 and 035: $tcnv\n";
                print STDERR $marc->as_formatted() . "\n";
                $current_title = "fishy: $tcnv";
            }
        }
    }
}
close BADMFHD;
close MFHD;
