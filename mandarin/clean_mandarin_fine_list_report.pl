#!/usr/bin/perl

use strict;
use warnings;

# copy and paste raw text from acrobat reader to .txt file

my $filename = $ARGV[0];
open(my $fh, '<:encoding(UTF-8)', $filename)
  or die "Could not open file '$filename' $!";

my $user;
 
while (my $row = <$fh>) {
  chomp $row;
  if ($row =~ m/Fines Due by patron/) { next; }
  if ($row =~ m/Patron Title Barcode Date Due Fine/) { next; }
  if ($row =~ m/Page: /) { next; }
  if ($row =~ m/Program Files \(x86\)/) { next; }
  if ($row =~ m/End Of Report/) { next; }
  if ($row =~ m/Fines Due by Patron/) { next; }

  my @str = split / /, $row;
  my $str_length = scalar(@str);
  if ($str[1] eq 'AM' or $str[1] eq 'PM') {
    if ($str[2] =~ m/2019/) { next; }	
  }

  if ($str[$str_length -1] !~ m/^\d*\.?\d*$/) {
    $user = $str[$str_length -1];
    next;
  }

  #print "$row\n";
  if ($str[$str_length -1] =~  m/^\d*\.?\d*$/) {
    my $f = $str[$str_length -1];  #fine
    my $i = $str[$str_length -3];  #item barcode 
    print "$user\t$i\t$f\n";
  } 
}
