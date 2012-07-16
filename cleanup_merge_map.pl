#!/usr/bin/perl

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


