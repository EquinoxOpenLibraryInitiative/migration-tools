#!/usr/bin/perl -w

# Converts a Unicorn users.data, bill.data, or charge.data file to a tab-separated file.
# 2009-08-10 Ben Ostrowsky <ben@esilibrary.com>

my @records;
my $serial = -1;
my $line = 0;
my $section = '';
my $field = '';
my %unique_fields;
my @output_fields = qw( l_form l_user_id l_item_id l_billing_lib l_bill_date l_bill_amt l_bill_reason );

# Load each record
while (<>) {
    s/\r\n/\n/g;
# print STDERR "Loaded this line: " . $_;

	# Is this the start of a new record?
	if ( /^... DOCUMENT BOUNDARY ...$/ ) {
		$line = 0;
		$serial++;
		$section = ''; # just in case this didn't get reset in the previous record
		# print STDERR "Processing record $serial.\n";
		next;
	}

	# Is this a FORM= line?
	if ( /^FORM=(.*)/ ) {
		$records[$serial]{'l_form'} = $1;
		next;
	}

	# If this isn't the start of the new record, it's a new line in the present record.
	$line++;

	# Is this line the beginning of a block of data (typically an address or a note)?
	if ( /^\.(.*?)_BEGIN.$/ ) {
		# print STDERR "I think this might be the beginning of a beautiful " . $1 . ".\n";
		$section = "$1.";
		next;
	}

	# Is this line the beginning of a block of data (typically an address or a note)?
	if ( /^\.(.*?)_END.$/ ) {
		if ("$1." ne $section) {
			print STDERR "Error in record $serial, line $line (input line $.): got an end-of-$1 but I thought I was in a $section block!\n";
		}
		# print STDERR "It's been fun, guys, but... this is the end of the " . $1 . ".\n";
		$section = '';
		next;
	}

	# Looks like we've got some actual data!  Let's store it.
	# FIXME: For large batches of data, we may run out of memory and should store this on disk.
	if ( /^\.(.*?).\s+(\|a)?(.*)$/ ) {

		# Build the name of this field (taking note of whether we're in a named section of data)
		$field = '';
		if ($section ne '') { 
			$field .= $section;
		}
		$field .= $1;

		# Now we can actually store this line of data!
#FIXME: Assign it manually to one of the l_fields in the SQL comment below.
		$records[$serial]{$field} = $3;		

		# print STDERR "Data extracted: \$records[$serial]{'$field'} = '$3'\n";

		next;
	}	

	# This is the continuation of the previous line.
	else {
		chomp($_);
		$records[$serial]{$field} .= ' ' . $_;
		# print STDERR "Appended data to previous field. \$records[$serial]{'$field'} is now '" . $records[$serial]{$field} . "'.\n";
	}

}

print STDERR "Loaded " . scalar(@records) . " records.\n";


#CREATE OR REPLACE FUNCTION migration_tools.unicorn_create_money_table (TEXT) RETURNS VOID AS $$
#    DECLARE
#        migration_schema ALIAS FOR $1;
#    BEGIN
#        PERFORM migration_tools.exec( $1, 'CREATE TABLE ' || migration_schema || '.money_grocery_unicorn (
#            l_form TEXT NOT NULL CHECK ( l_form = ''LDBILL'' ),
#            l_user_id TEXT,
#            l_item_id TEXT,
#            l_billing_lib TEXT,
#            l_bill_date TEXT,
#            l_bill_amt TEXT,
#            l_bill_reason TEXT
#        ) INHERITS ( ' || migration_schema || '.money_grocery);' );
#    END;
#$$ LANGUAGE PLPGSQL STRICT STABLE;


# Print a header line
print "SERIAL\t";
foreach $i (@output_fields) {
	print "$i\t";
}
print "\n";


# Print the results
for (my $u = 0; $u < @records; $u++) {
	foreach $f (@output_fields) {
		if (defined $records[$u]{$f}) {
			print $records[$u]{$f};
		}
	print "\t";
	}
	print "\n";
}

print STDERR "Wrote " . scalar(@records) . " records.\n";
# uh-bdee-uh-bdee-uh-bdee-uh- THAT'S ALL, FOLKS
