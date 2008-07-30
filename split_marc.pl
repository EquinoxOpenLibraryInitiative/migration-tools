#!/usr/bin/perl
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use POSIX;

my $split_every = $ARGV[0];
my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach $argnum ( 1 .. $#ARGV ) {

        print STDERR "Processing " . $ARGV[$argnum] . "\n";

        my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
        $batch->strict_off();
        $batch->warnings_off();

        while ( my $record = $batch->next() ) {

        $count++;

                my $filename = $ARGV[$argnum] . ".split." .  floor( $count / $split_every ) . ".xml";

                open FILE, ">>$filename";
                binmode(FILE, ':utf8');
                print FILE $record->as_xml();
                close FILE;
        }
    print STDERR "Processed $count records.\n";
}

