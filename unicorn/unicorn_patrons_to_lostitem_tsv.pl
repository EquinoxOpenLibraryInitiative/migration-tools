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
