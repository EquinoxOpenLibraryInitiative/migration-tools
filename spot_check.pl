#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach $argnum ( 0 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

	my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
	$batch->strict_off();
	$batch->warnings_off();

    my $last_successful_record;

    eval {
        while ( my $record = $batch->next() ) {

            $count++; 

            $last_successful_record = $record->as_xml();

            print STDERR "WARNINGS: Record $count : " . join(":",@warnings) . " : continuing...\n" if ( @warnings );

        }
    };
    print STDERR "Processed $count records.  Last successful record = " . $last_successful_record . "\n";
    warn $@ if $@;
}
