#!/usr/bin/perl -w
# ./marc_grep.pl tag subfield value marcfile > out 2> err
# Spit out records that contain the tag/subfield/value combination provided

use strict;
use warnings;

use MARC::Batch;

my ($tag,$subfield,$value,$file) = @ARGV;

my $batch = MARC::Batch->new( 'USMARC', $file );
$batch->strict_off();
$batch->warnings_off();


while ( my $marc = $batch->next ) {
    my $found_match = 0;
    foreach my $f ($marc->fields()) {
        if ($f->tag() eq $tag && $f->subfield($subfield) eq $value) {
            $found_match = 1;
        }
    }
    if ($found_match) {
        print $marc->as_usmarc();
    }
}
