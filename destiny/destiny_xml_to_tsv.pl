#!/usr/bin/perl -w

# Copyright 2009-2016, Equinox Software, Inc.
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

# Take a file that starts off like this:
#
# <?xml version="1.0" encoding="UTF-8" ?>
# <DestinyCustomReport>
#  <Row>
#
# Munge it like so:
#
# cat file | xml2 | cut -f4- -d/  > file.munged
#
# And feed the results to this script to create a .tsv version of the data.
#
# destiny_xml_to_tsv.pl file.munged > file.munged.tsv
#


my @records;
my $serial = 0;
my $line = 0;
my $field = '';
my %unique_fields;


# Load each record
while (<>) {
    s/\r\n/\n/g;
# print STDERR "Loaded this line: " . $_;

	# Is this the start of a new record?
	if ( /^$/ ) {
		$line = 0;
		$serial++;
		print STDERR "Processing record $serial.\n";
		next;
	}

	# If this isn't the start of the new record, it's a new line in the present record.
	$line++;

	# Looks like we've got some actual data!  Let's store it.
	# FIXME: For large batches of data, we may run out of memory and should store this on disk.
	if ( /^(.*?)=(.*)$/ ) {

		$field = $1;
		$unique_fields{$field} = 1;
		$records[$serial]{$field} = $2;

		print STDERR "Data extracted: \$records[$serial]{'$field'} = '$2'\n";

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
