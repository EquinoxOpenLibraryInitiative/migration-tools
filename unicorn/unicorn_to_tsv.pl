#!/usr/bin/perl -w

# Copyright 2009-2012, Equinox Software, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

# Converts a Unicorn users.data, bill.data, or charge.data file to a tab-separated file.
# 2009-08-10 Ben Ostrowsky <ben@esilibrary.com>

my @records;
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
		print STDERR "Processing record $serial.\n";
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
		print STDERR "I think this might be the beginning of a beautiful " . $1 . ".\n";
		$section = "$1.";
		next;
	}

	# Is this line the beginning of a block of data (typically an address or a note)?
	if ( /^\.(.*?)_END.$/ ) {
		if ("$1." ne $section) {
			print STDERR "Error in record $serial, line $line (input line $.): got an end-of-$1 but I thought I was in a $section block!\n";
		}
		print STDERR "It's been fun, guys, but... this is the end of the " . $1 . ".\n";
		$section = '';
		next;
	}

	# Looks like we've got some actual data!  Let's store it.
	# FIXME: For large batches of data, we may run out of memory and should store this on disk.
	if ( /^\.([A-Z0-9_\/]+?)\.\s+(\|a)?(.*)$/ ) {

		# Build the name of this field (taking note of whether we're in a named section of data)
		$field = '';
		if ($section ne '') { 
			$field .= $section;
		}
		$field .= $1;

		# Store the field as a key of an array.  If it already exists, oh well, now it still exists.
		$unique_fields{$field} = 1;

		# Now we can actually store this line of data!
		# If it's a repeating field, concatenate the data with semicolons
		if (defined $records[$serial]{$field}) {
			print STDERR "Repeating field found: $field\n";
			$records[$serial]{$field} .= ";$3";
		} else {
			$records[$serial]{$field} = $3;
		}

		print STDERR "Data extracted: \$records[$serial]{'$field'} = '$3'\n";

		next;
	}	

	# This is the continuation of the previous line.
	else {
		chomp($_);
		$records[$serial]{$field} .= ' ' . $_;
		print STDERR "Appended data to previous field. \$records[$serial]{'$field'} is now '" . $records[$serial]{$field} . "'.\n";
	}

}

print STDERR "Loaded " . scalar(@records) . " records.\n";


# Print a header line
print "SERIAL\t";
@sorted_fields = sort keys %unique_fields;
foreach $i (@sorted_fields) {
	print "$i\t";
}
print "\n";


# Print the results
for (my $u = 0; $u < @records; $u++) {
	print "$u\t";	
	foreach $f (@sorted_fields) {
		if (defined $records[$u]{$f}) {
			print $records[$u]{$f};
		}
	print "\t";
	}
	print "\n";
}

print STDERR "Wrote " . scalar(@records) . " records.\n";
# uh-bdee-uh-bdee-uh-bdee-uh- THAT'S ALL, FOLKS
