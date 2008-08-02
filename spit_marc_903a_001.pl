#!/usr/bin/perl
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

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

        my @my001 = $record->field('001');
        if (scalar(@my001) == 0 || scalar(@my001) > 1) { die "Wrong number of 001 tags for record $count\n"; }
        my @my903 = $record->field('903');
        if (scalar(@my903) == 0 || scalar(@my903) > 1) { die "Wrong number of 903 tags for record $count\n"; }
        print $my903[0]->subfield('a') . "\t" . $my001[0]->as_string() . "\n"
	}
	print STDERR "Processed $count records\n";
}
