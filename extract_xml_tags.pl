#!/usr/bin/perl -w
use strict;

use Getopt::Long;

my (@tags, $infile);
GetOptions ("tags=s" => \@tags,
            "infile=s" => \$infile);
@tags = split(/,/, join(',', @tags));

open(FH, $infile) or die "Can't open $infile for reading: $!";

while (<FH>) { 

  my %tag;
  my $xml = $_;

  # Find the Evergreen bib ID
  $xml =~ m/<datafield tag="903".+?<subfield code="a">(.+?)<\/subfield>/; 
  my $egid = $1; 

  # Find each occurrence of each tag specified
  foreach (@tags) {
    $tag{$_} = [ $xml =~ m/(<datafield tag="$_".+?<\/datafield>)/g ];
  }

  # Clean up the results before printing
  my $output = '';
  foreach my $key (sort keys %tag) {
    my $text = join("", @{$tag{$key}});
    $text =~ s/>\s+</></g;
    $output .= $text;
  }

  # If we found any specified tags, print what we found.
  if ($output ne '') {
    print "$egid\t$output\n";
  }

}

close(FH);
