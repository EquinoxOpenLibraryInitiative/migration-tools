#!/usr/bin/perl -w
use strict;

use MARC::File::USMARC;

my $file = MARC::File::USMARC->in( $ARGV[0] );
while ( my $marc = $file->next() ) {
    my @cns = $marc->field('001'); # grabs all of them
    my $cn;
    if (@cns) {
        $cn = $marc->field('001')->data(); # grabs the first
        $marc->delete_fields(@cns); # deletes all of them
    }
    my @sources = $marc->field('003'); # etc
    my $source;
    if (@sources) {
        $source = $marc->field('003')->data();
        $marc->delete_fields(@sources);
    }
    my @tags035 = $marc->field('035');
    my $tag035 = $marc->field('035');
    my $tag035a = defined $tag035 ? $tag035->subfield('a') : undef;
    $marc->delete_fields(@tags035);
    if (defined $cn) {
        my @arr = (
            '035','','','a'

        );
        if (defined $source) {
           push @arr, "($source) $cn";
        } else {
           push @arr, "$cn";
        }
        if (defined $tag035a) {
            push @arr, 'z';
            push @arr, $tag035a;
        }
        my $new035 = MARC::Field->new(@arr);
        $marc->insert_fields_ordered($new035);
    }
    print $marc->as_usmarc();
}
$file->close();
undef $file;
