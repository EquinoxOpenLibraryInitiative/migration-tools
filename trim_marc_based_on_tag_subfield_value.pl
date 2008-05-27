#!/usr/bin/perl
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;


my $tag_number = $ARGV[0];
my $tag_subfield = $ARGV[1];
my $tag_value = $ARGV[2];

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach $argnum ( 3 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

	my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
	$batch->strict_off();
	$batch->warnings_off();

	while ( my $record = $batch->next() ) {

        $count++;

		print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );

        my $keep_me = 0;

        my @tags = ();
		my @tags; if ($record->field($tag_number)) { @tags = $record->field($tag_number); }
		foreach my $f ( @tags ) { 
            if ($f->subfield($tag_subfield)) { 
                if ( $f->subfield($tag_subfield)=~ m/($tag_value)/i ) { $keep_me = 1; } 
            } 
        }

        if ($keep_me) {
            print STDOUT $record->as_xml();
        }

	}
	print STDERR "Processed $count records\n";
}
