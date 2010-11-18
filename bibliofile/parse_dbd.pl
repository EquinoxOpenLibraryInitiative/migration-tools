#!/usr/bin/perl -w

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





