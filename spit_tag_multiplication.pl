#!/usr/bin/perl
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
#use MARC::Field;
use Unicode::Normalize;

my $filename = $ARGV[0];
my $tag1 = $ARGV[1];
my $subfield1 = $ARGV[2];
my $tag2 = $ARGV[3];
my $subfield2 = $ARGV[4];

die "required arguments: filename tag1 subfield1 tag2 subfield2\n" if (! ($filename && $tag1 && $subfield1 && $tag2 && $subfield2) );

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

print STDERR "Processing $filename\n";

my $batch = MARC::Batch->new('XML',$filename); $batch->strict_off(); $batch->warnings_off();

while ( my $record = $batch->next() ) {

    $count++;

    print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );

    my @tags1 = (); if ($record->field($tag1)) { @tags1 = $record->field($tag1); } else { next; }

    foreach my $f1 ( @tags1 ) {
        if ($f1->subfield($subfield1)) {
            my @subfields1 = $f1->subfield($subfield1);
            foreach my $s1 ( @subfields1 ) {
            #***********************************************************************************************************************

                my @tags2 = (); if ($record->field($tag2)) { @tags2 = $record->field($tag2); } else { next; }

                foreach my $f2 ( @tags2 ) {
                    if ($f2->subfield($subfield2)) {
                        my @subfields2 = $f2->subfield($subfield2);
                        foreach my $s2 ( @subfields2 ) {
                            print "$s1\t$s2\n";
                        }
                    }
                }

            #***********************************************************************************************************************
            }
        }
    }
}
print STDERR "Processed $count records\n";
