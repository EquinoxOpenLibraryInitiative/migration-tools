#!/usr/bin/perl

use strict;
use warnings;

=head1 NAME

cleanup_merge_map.pl

=head2 SUMMARY

Little helper script used when consoldating
multiple merge maps.

=cut

my %bad_subs = ();
my %map = ();
while (<>) {
    chomp;
    my ($lead, $sub) = split /\t/, $_, -1;
    next if exists $bad_subs{$sub};
    if (exists $map{$sub}) {
        $bad_subs{$sub}++;
        delete $map{$sub};
        next;
    }
    $map{$sub} = $lead;
}
foreach my $sub (sort keys %map) {
    print "$map{$sub}\t$sub\n";
}


