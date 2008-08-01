#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

print join("\t",
    "bib id",
    "library",
    "barcode",
    "current location",
    "home location",
    "call number",
    "item type",
    "acq date",
    "price",
    "circulate flag",
    "total charges",
    "cat1",
    "cat2"
) . "\n";

foreach my $argnum ( 0 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

    my $M;
    open $M, '<:utf8', $ARGV[$argnum];
    my $batch = MARC::Batch->new('XML',$M);

	$batch->strict_off();
	$batch->warnings_off();

	while ( my $record = $batch->next() ) {

        $count++;

		print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );
        my $my_903a = $record->field('903')->subfield('a'); # target bib id's here
        my @tags = $record->field('999');
        foreach my $tag ( @tags ) {
            print join("\t",
                $my_903a,
                $tag->subfield('m') || '', # library
                $tag->subfield('i') || '', # barcode
                $tag->subfield('k') || '', # current location
                $tag->subfield('l') || '', # home location
                $tag->subfield('a') || '', # call number
                $tag->subfield('t') || '', # item type
                $tag->subfield('u') || '', # acq date
                $tag->subfield('p') || '', # price
                $tag->subfield('r') || '', # circulate flag
                $tag->subfield('n') || '', # total charges
                $tag->subfield('x') || '', # cat1
                $tag->subfield('z') || ''  # cat2
            ) . "\n";
        }

	}
	print STDERR "Processed $count records\n";
}
