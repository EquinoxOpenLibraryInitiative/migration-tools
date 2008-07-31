#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use POSIX;
use Error qw/:try/;

my $split_every = $ARGV[0];
my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');
my $M;

foreach $argnum ( 1 .. $#ARGV ) {
	
	open $M, '<:utf8', $ARGV[$argnum];

        print STDERR "Processing " . $ARGV[$argnum] . "\n";

        my $batch = MARC::Batch->new('XML', $M);
        $batch->strict_off();
        $batch->warnings_off();

	my $record;
        while ( try { $record = $batch->next() } otherwise { $record = -1 } ) {
		next if ($record == -1);
		$count++;

                my $filename = $ARGV[$argnum] . ".split." .  floor( $count / $split_every ) . ".xml";

                open FILE, ">>$filename";
                binmode(FILE, ':utf8');
                print FILE $record->as_xml();
                close FILE;

		$record = undef;

	            unless ($count % 1000) {
        	        print STDERR "$count\r"
            	}

        }
    print STDERR "Processed $count records.\n";
}

