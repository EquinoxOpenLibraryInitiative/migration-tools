#!/usr/bin/perl
use MARC::Batch;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;

my $initial_record_number = $ARGV[0];
my $record_tag_number = $ARGV[1];
my $record_tag_subfield = $ARGV[2];
my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach $argnum ( 3 .. $#ARGV ) {

	print STDERR "Processing " . $ARGV[$argnum] . ", starting record id at $initial_record_number\n";

	my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
	#$batch->strict_off();
	#$batch->warnings_off();

	while ( my $record = $batch->next() ) {

        $count++;

        print STDERR "WARNINGS: Record $count : " . join(":",@warnings) . " : continuing...\n" if ( @warnings );

        while ($record->field($record_tag_number)) { $record->delete_field( $record->field($record_tag_number) ); }
        my $new_id = $initial_record_number + $count - 1;
        my $new_id_field = MARC::Field->new( $record_tag_number, ' ', ' ', $record_tag_subfield => $new_id);
        $record->append_fields($new_id_field);

        print $record->as_xml();
	}

    print STDERR "Processed $count records.  Last record id at " . ($initial_record_number + $count - 1) . "\n";
}
