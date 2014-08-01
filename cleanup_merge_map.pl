#!/usr/bin/perl

# Copyright 2011-2014, Equinox Software, Inc.
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
use warnings;

=head1 NAME

cleanup_merge_map.pl

=head2 SUMMARY

Little helper script used when consoldating
multiple merge maps.

=cut

my %leads = ();

# load merge map
while (<>) {
    chomp;
    my ($lead, $sub) = split /\t/, $_, -1;
    $leads{$sub}->{$lead}++; 
}

# run this twice to ensure that cycles are
# excluded
cleanup_map() foreach (1..2);

foreach my $sub (sort numerically keys %leads) {
    if (1 == keys(%{ $leads{$sub} })) {
        print join("\t", keys(%{ $leads{$sub} }), $sub), "\n";
    }
}

sub cleanup_map {
    foreach my $sub (keys %leads) {
        my @leads_to_prune = ();
        my @leads_to_add = ();
        foreach my $lead (keys %{ $leads{$sub} }) {
            if (exists($leads{$lead})) {
                # lead bib itself is slated to be merged,
                # so it's no longer going to be the direct
                # lead for the current sub
                push @leads_to_prune, $lead;

                # the current sub gets potential
                # leads from its previous lead
                foreach my $second_lead (keys %{ $leads{$lead} }) {
                    push @leads_to_add, $second_lead unless exists($leads{$sub}->{$second_lead});
                }
            }
        }
        delete($leads{$sub}->{$_}) foreach @leads_to_prune;
        $leads{$sub}->{$_}++ foreach @leads_to_add;
    }
}

sub numerically {
    return $a <=> $b;
}
