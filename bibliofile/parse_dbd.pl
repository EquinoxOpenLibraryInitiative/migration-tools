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

use strict;

$/ = undef;

my %coltypes = ( 
  'A' => 'Text',
  'N' => 'Numeric',
  'S' => 'Integer'
);
my $startOfColumnTypes = 8;

while (<>) {

  my $dbd = $_;
  my $rowlength = ord(substr($dbd, 0, 1)) + (256 * (ord(substr($dbd, 1, 1))));
  my $numcolumns = ord substr($dbd, 2, 1);
  my $extra = sprintf(
    "%02x %02x %02x", 
    ord substr($dbd, 4, 1),
    ord substr($dbd, 5, 1),
    ord substr($dbd, 6, 1),
  );
  my $delimiter = sprintf(
    "%02x",
    ord substr($dbd, 7, 1),
  );

  my $colnames = substr($dbd, $startOfColumnTypes + 7*$numcolumns - 2);
  my @col = split(/\x00/, $colnames);

  print "Row length: $rowlength\n";
  print "Columns:    $numcolumns\n";
  print "Extra data: $extra\n";
  print "Delimiter:  $delimiter\n";

  for (my $i = 1; $i <= $numcolumns; $i++) {
    my $coltype = substr($dbd, 7*($i-1)+$startOfColumnTypes, 1);
    my $collength = ord substr($dbd, 7*($i-1)+$startOfColumnTypes+1, 1);
    printf ("Column %02d: %-8s %s (%d chars)\n", $i, $coltypes{$coltype}, $col[$i-1], $collength);
  }

}





