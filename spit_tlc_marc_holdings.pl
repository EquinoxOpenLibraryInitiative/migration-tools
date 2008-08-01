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
	"collection code",
	"prefix",
	"call number",
	"cutter number",
	"barcode",
	"serial year",
	"volume number",
	"part subdivision 1",
	"part subdivision 2",
	"part subdivision 3",
	"part subdivision 4",
	"copy number",
	"accession number",
	"price",
	"condition",
	"magnetic media",
	"checkin-in/check-out note"
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
        my @tags = $record->field('949');
        foreach my $tag ( @tags ) {
            print join("\t",
                $my_903a, # bib id
                $tag->subfield('a') || '', # collection code
                $tag->subfield('b') || '', # prefix
                $tag->subfield('c') || '', # call number
                $tag->subfield('d') || '', # cutter number
                $tag->subfield('g') || '', # barcode
                $tag->subfield('h') || '', # serial year
                $tag->subfield('i') || '', # volume number
                $tag->subfield('j') || '', # part subdivision 1
                $tag->subfield('k') || '', # part subdivision 2
                $tag->subfield('l') || '', # part subdivision 3
                $tag->subfield('m') || '', # part subdivision 4
                $tag->subfield('n') || '', # copy number
                $tag->subfield('o') || '', # accession number
                $tag->subfield('p') || '', # price
                $tag->subfield('q') || '', # condition
                $tag->subfield('5') || '', # magnetic media 
                $tag->subfield('7') || '' # checkin-in/check-out note
            ) . "\n";
        }

	}
	print STDERR "Processed $count records\n";
}
