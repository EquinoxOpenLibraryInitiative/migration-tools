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

use Test::More tests => 3;
use Equinox::Migration::Utils qw/normalize_oclc_number/;

is(normalize_oclc_number('ocm38548133'),        '(OCoLC)38548133', 'prefixed with "ocm"');
is(normalize_oclc_number('   ocm38548133    '), '(OCoLC)38548133', 'ignore leading/trailing whitespace');
is(normalize_oclc_number('(OCoLC)ocm00123456'), '(OCoLC)123456',   'ignore leading zeroes in number');
