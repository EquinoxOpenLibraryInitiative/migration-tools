#!/usr/bin/perl
use open ':utf8';
use Error qw/:try/;
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

	my $record = -1;
	while ( try { $record = $batch->next() } otherwise { $record = -1 } ) {
		next if ($record == -1);

        $count++;

		print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );
        my $my_903a = $record->field('903')->subfield('a'); # target bib id's here
        my @tags = $record->field('852');
        foreach my $tag ( @tags ) {
                my $lib = $tag->subfield(''); # library
                my $barcode = $tag->subfield('p'); # barcod
                my $loc = $tag->subfield(''); # current location
                my $home_loc = $tag->subfield('c'); # home location
                my $cn = $tag->subfield('h'); # call number
                my $type = $tag->subfield(''); # item type
                my $create_date = $tag->subfield(''); # acq date
                my $price = $tag->subfield('9'); # price
		$price =~ s/[^0-9\.]//g;
                my $circ_flag = $tag->subfield(''); # circulate flag
                my $total_circ = $tag->subfield(''); # total charges
                my $cat1 = $tag->subfield(''); # cat1
                my $cat2 = $tag->subfield(''); # cat2
            print join("\t",
                $my_903a, $lib, $barcode, $loc, $home_loc,
                $cn, $type, $create_date, $price, $circ_flag,
                $total_circ, $cat1, $cat2) . "\n";
        }

	}
	print STDERR "Processed $count records\n";
}
