#!/usr/bin/perl -w

# Converts a Unicorn users.data file to a tab-separated file.
# 2009-08-10 Ben Ostrowsky <ben@esilibrary.com>

my @users;
my $serial = -1;
my $line = 0;
my $section = '';
my $field = '';
my %unique_fields;


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

		# Store the field as a key of an array.  If it already exists, oh well, now it still exists.
		$unique_fields{$field} = 1;

		# Now we can actually store this line of data!
		$users[$serial]{$field} = $3;		

		# print STDERR "Data extracted: \$users[$serial]{'$field'} = '$3'\n";

		next;
	}	

	# This is the continuation of the previous line.
	else {
		chomp($_);
		$users[$serial]{$field} .= ' ' . $_;
		# print STDERR "Appended data to previous field. \$users[$serial]{'$field'} is now '" . $users[$serial]{$field} . "'.\n";
	}

}

print STDERR "Loaded " . scalar(@users) . " users.\n";


# Print a header line
print "SERIAL\t";
@sorted_fields = sort keys %unique_fields;
foreach $i (@sorted_fields) {
	print "$i\t";
}
print "\n";


# Print the results
for (my $u = 0; $u < @users; $u++) {
	print "$u\t";	
	foreach $f (@sorted_fields) {
		if (defined $users[$u]{$f}) {
			print $users[$u]{$f};
		}
	print "\t";
	}
	print "\n";
}

print STDERR "Wrote " . scalar(@users) . " users.\n";
# uh-bdee-uh-bdee-uh-bdee-uh- THAT'S ALL, FOLKS
