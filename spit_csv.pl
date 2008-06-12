#!/usr/bin/perl
use MARC::Batch;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );
use MARC::Field;
use Unicode::Normalize;


my @desired_tags_subfields = ();
foreach my $argnum ( 1 .. $#ARGV) {
    print STDERR $ARGV[$argnum] . "\n";
    push @desired_tags_subfields, $ARGV[$argnum];
}

my $count = 0;

binmode(STDOUT, ':utf8');
binmode(STDIN, ':utf8');

foreach my $argnum ( 0 .. 0 ) {

	print STDERR "Processing " . $ARGV[$argnum] . "\n";

	my $batch = MARC::Batch->new('XML',$ARGV[$argnum]);
	$batch->strict_off();
	$batch->warnings_off();

	while ( my $record = $batch->next() ) {

        $count++;

		print STDERR "WARNINGS: Record $count : " .  join(":",@warnings) . " : continuing...\n" if ( @warnings );

        my $first = 1;
        for (my $i = 0; $i < scalar(@desired_tags_subfields); $i+=2) {
		    my @tags = (); 
            if ($record->field($desired_tags_subfields[$i])) { 
                @tags = $record->field($desired_tags_subfields[$i]); 
            }
            if (scalar(@tags)>1) { 
                die "Multiple $desired_tags_subfields[$i]\n"; 
            } elsif (scalar(@tags)==0) { 
                print STDERR "Record $count missing $desired_tags_subfields[$i]\n"; 
                goto END_OF_WHILE; 
            }
            foreach my $f ( @tags ) { 
                if ($f->subfield($desired_tags_subfields[$i+1])) { 
                    if ($first) {
                        $first = 0;
                    } else {
                        print STDOUT "\t";
                    }
                    print STDOUT $f->subfield($desired_tags_subfields[$i+1]);
                } 
            }
        }
        print STDOUT "\n";
        END_OF_WHILE:
	}
	print STDERR "Processed $count records\n";
}
