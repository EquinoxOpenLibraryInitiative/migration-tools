#!/usr/bin/perl
use open ':utf8';
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;

my $format = $ARGV[0];

my $record_id_file = $ARGV[1];
my %record_ids;

open FILE, $record_id_file;
while (my $record_id = <FILE>) {
    chomp($record_id); $record_ids{ $record_id } = 1;
}
close FILE;

my $id_tag = $ARGV[2]; my $id_subfield = $ARGV[3];

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

my $M;

foreach $argnum ( 4 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

    open $M, '<:utf8', $ARGV[$argnum];

	my $batch = MARC::Batch->new('XML',$M);
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

        if (! defined $record_ids{ $id }) {
            if ($format eq 'text') {
                print STDOUT '=-' x 39 . "\n";
                print STDOUT $record->as_formatted() . "\n";
            } else {
                print STDOUT $record->as_xml() . "\n";
            }
        }
	}
    print STDERR "Processed $count records.\n";
}
