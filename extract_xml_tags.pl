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
