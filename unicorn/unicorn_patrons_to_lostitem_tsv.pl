#!/usr/bin/perl -w

# Converts a Unicorn users.data file to a tab-separated file of lost items.
# 2009-11-06 Ben Ostrowsky <ben@esilibrary.com>
#
# Output fields:
#
#   Patron ID
#   Item ID
#   Item Copy Number
#   Due Date
#   Title, Author, Call (or parts thereof)
#

my $field = '';
my $lostitem = '';
my $userid = '';

# Load each record
while (<>) {
    s/\r\n/\n/g;
# print STDERR "Loaded this line: " . $_;

	if ( /^\.(.*?).\s+(\|a)?(.*)$/ ) {
		$field = $1;
		if ($field eq 'USER_ID') { 
			if ($lostitem ne '') { 
				$lostitem =~ m/^(.*)copy:([^,]*),\s*ID:([^,]*),\s*due:(.*)$/;
				print "$userid\t$3\t$2\t$4\t$1\n"; 
			}
			$userid = $3;
			$lostitem = '';
		}
		if ($field eq 'LOSTITEM') { 
			if ($lostitem ne '') { 
				$lostitem =~ m/^(.*)copy:([^,]*),\s*ID:([^,]*),\s*due:(.*)$/;
				print "$userid\t$3\t$2\t$4\t$1\n"; 
			}
			$lostitem = $3;
		} 
		next;
	}	

	# This is the continuation of the previous line.
	else {
		chomp($_);
		if ($field eq 'LOSTITEM') { $lostitem .= $_; }
	}

}
