#!/usr/bin/perl -w

# Parses Bibliofile files.
# Usage: parse_db.pl TITLE.DB
# Works fine on TITLE.DB, but misses the boat on other files; probably different block sizes or something.

use strict;
use POSIX;

$/ = undef;

my $startOfColumnTypes = 8;
my $startOfRealData = 4096;
my $blockSize = 4096;
my $initialOffset = 6;
my %dataTypes = (
  'A' => 'Text',
  'N' => 'Numeric',
  'S' => 'Integer'
);

my $rowLength;
my @fieldLengths;
my @fieldNames;
my @fieldTypes;

my $db = $ARGV[0];
my $dbd = $db . "D";

open (DBD, $dbd);

while (<DBD>) {

  my $data = $_;

  $rowLength = ord(substr($data, 0, 1)) + (256 * (ord(substr($data, 1, 1))));
  #print STDERR "Row length: $rowLength\n";

  my $numColumns = ord substr($data, 2, 1);
  #print STDERR "Columns:    $numColumns\n";

  my $namedata = substr($data, $startOfColumnTypes + ($numColumns * 7) - 2);
  @fieldNames = split(/\x00/, $namedata);
 
  for (my $i = 0; $i < $numColumns; $i++) {
    $fieldTypes[$i] = substr($data, ($i * 7) + $startOfColumnTypes, 1);
    $fieldLengths[$i] = ord substr($data, ($i * 7) + $startOfColumnTypes + 1, 1);
  }

}

close(DBD);

print join("\t", @fieldNames) . "\n";

open (DB, $db);

my $blocks = 0;

while (read DB, my $data, $blockSize) {
  $blocks++;
  next if ($blocks == 1);
  my $maxRecords = POSIX::floor($blockSize / $rowLength);
  my $indexIndicator1 = ord substr($data, 1, 1);
  next if ($indexIndicator1 != 0);
  my $indexIndicator2 = ord substr($data, 7, 1);
  next if ($indexIndicator2 == 0);

#  for (my $i = 1; $i <= scalar(@fieldLengths); $i++) {
#    print "Field $i has length $fieldLengths[$i-1]\n";
#  }

  for (my $r = 0; $r < $maxRecords; $r++) {

    my $pos = 0;
    my @field;

    #print STDERR "Record " . ($r+1) . " of $maxRecords\n";


    for (my $f = 0; $f < scalar(@fieldLengths); $f++) {
      $field[$f] = substr($data, $initialOffset + ($r * $rowLength) + $pos, $fieldLengths[$f]);
      if ($fieldTypes[$f] eq 'S') { $field[$f] = ord $field[$f]; }
      $pos += $fieldLengths[$f];
    }

    if ($field[0] =~ m/[^\x00]/) {
      print join("\t", @field) . "\n";
      #print STDERR "Length: $pos\n";
    }

  }

}

close(DB);
