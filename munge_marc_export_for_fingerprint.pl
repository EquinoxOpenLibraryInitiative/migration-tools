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

# Utility script to prepare a file of MARCXML records extracted from an Evergreen
# database for fingerprinter by adding 903 fields.  Usage:
#  echo "select id || chr(9) || REGEXP_REPLACE(marc, E'\\n','','g') from biblio.record_entry where not deleted and id < $BIBIDSTART" > $BIN/incumbent_bibs.sql 
#  psql -A -t -U $DBUSER < $BIN/incumbent_bibs.sql | munge_marc_export_for_fingerprint.pl > $INTER/incumbent.mrc

while (<>) {
    my ($id, $rest) = split /\t/, $_, 2;
    $rest =~ s!<datafield .*?tag="903".*?</datafield>!!g;
    $rest =~ s!</record>!<datafield tag="903"><subfield code="a">$id</subfield></datafield></record>!;
    $rest =~ s!</marc:record>!<datafield tag="903"><subfield code="a">$id</subfield></datafield></marc:record>!;
    print $rest;
}

