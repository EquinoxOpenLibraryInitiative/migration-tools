#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;

my $record_id_file = $ARGV[0];
my %record_ids;

open FILE, $record_id_file;
while (my $record_id = <FILE>) {
    chomp($record_id); $record_ids{ $record_id } = 1;
}
close FILE;

my $id_tag = $ARGV[1]; my $id_subfield = $ARGV[2];

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach $argnum ( 3 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

	my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
	$batch->strict_off();
	$batch->warnings_off();

    my $count = 0;

	while ( my $record = $batch->next() ) {

        $count++;

		my $id = $record->field($id_tag);
		if (!$id) {
			print STDERR "ERROR: This record is missing a $id_tag field.\n" . $record->as_formatted() . "\n=====\n";
			next;
		}
		$id = $id->as_string($id_subfield);

        if (defined $record_ids{ $id }) {
            open FILE, ">$id";
            binmode(FILE, ':utf8');
            print FILE $record->as_xml();
            close FILE;
        }
	}
    print STDERR "Processed $count records.\n";
}
