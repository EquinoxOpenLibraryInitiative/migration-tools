#!/usr/bin/perl

use strict;
use warnings;

use XBase;  # or could use DBI and DBD::XBase;
use Data::Dumper;
use Getopt::Long;
use Encode;

my $in = '';
my $out = '';
GetOptions('in=s' => \$in, 'out=s' => \$out);

open OUT, ">$out" or die $!;

my $table = new XBase $in or die XBase->errstr;

# get list of field names
my @names = $table->field_names;

# dump PATRONID, SURNAME, FIRSTNAME
print OUT join ("\t", @names) . "\n";

sub clean {
  if ( $_ ) { 
    s/\\/\\\\/g;
    s/\n/\\n/g; 
    s/\r/\\r/g; 
    s/\t/\\t/g; 
    Encode::encode("utf8", $_) 
  } else { ''; } # to avoid 'Use of uninitialized value in join'
}

my $i = 0;
for (0 .. $table->last_record) {
    $i++;
    my ($deleted, @row) = $table->get_record($_);
    @row = map (&clean, @row); 
    print OUT join("\t", @row) . "\n" unless $deleted;

}

print STDERR "$i records exported to $out.\n";
