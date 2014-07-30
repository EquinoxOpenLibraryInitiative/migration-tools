package Equinox::Migration::Utils;

# Copyright 2014, Equinox Software, Inc.
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

BEGIN {
    require Exporter;
    
    our $VERSION =   1.00;
    our @ISA     =   qw(Exporter);
    our @EXPORT  =   ();
    our @EXPORT_OK = qw(normalize_oclc_number);
}

sub normalize_oclc_number {
    my $str = shift;
  
    # trim
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;

    # get rid of prefixes
    $str =~ s/^\(OCoLC\)//i;
    $str =~ s/^(ocl7|ocm|ocn|on)//i; 

    # ... and any leading zeroes
    $str =~ s/^0+//;

    if ($str =~ /^\d+$/) {
        return '(OCoLC)' . $str;
    } else {
        return;
    }
}

=head1 NAME

Equinox::Migration::Utils - utility functions

=head1 SYNOPSIS

  use Equinox::Migration::Utils qw/normalize_oclc_number/;
  my $normalized = normalize_oclc_number($oclc);

=head1 FUNCTIONS

=head2 normalize_oclc_number)

  my $normalized = normalize_oclc_number($oclc);

Returns a normalized form of a string that is assumed to be
an OCLC control number. The normalized form consists of the
string "(OCoLC)" followed by the numeric portion of the OCLC
number, sans leading zeroes.

The input string is expected to be a sequence of digits with
optional leading and trailing whitespace and an optional prefix
from a set observed in the wild, e.g., "(OCoLC)", "ocm", and so
forth. If the input string does not meet this condition, the
undefined value is returned.

=head1 AUTHOR

Galen Charlton

=head1 COPYRIGHT

Copyright 2014, Equinox Software Inc.

=cut

1;
