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

require 5.10.0;

# This is still very rough.

use Getopt::Std;
use Data::Dumper;
use MARC::Field;
use JSON::XS;

use utf8;

# order of field separators, i think:   ! ; : .

# pubcycle_to_scap() was derived at late hours, and isn't totally right.  It's
# also not commented yet as you can see.  Sorry, but this still needs lots of
# work to be reliable and understandable.
#
# $record is one of the objects loaded from serctl.data.  $field is a
# MARC::Field object.
sub pubcycle_to_scap {
    my ($record, $field) = @_;

    if ($record->{NAM_TYPE} ne 'NUMERATION') {
        $field->update(i => '(year)');
        $field->update(x => '01');

        if ($record->{NAM_TYPE} =~ /SEASON/) {
            $field->update(j => '(season)');
        } elsif ($record->{NAM_TYPE} =~ /MONTH|DATE/) {
            $field->update(j => '(month)');

            $field->update(k => '(day)') if $record->{NAM_TYPE} eq 'DATE';
        }
    }

    my @pfields = split /\!/, $record->{PUBCYCLE_DEF};

    my @dow = qw/su mo tu we th fr sa/;
    my @yf = qw/X a g h/;
    my %periods_mo = ( 1 => 'm', 2 => 'b', 3 => 'q', 6 => 'f' );

    if (@pfields == 2) {
        if ($pfields[0] =~ /^(\d):M$/ and $pfields[1] =~ /^\d+:D$/ and $record->{NAM_TYPE} =~ /MONTH|CUSTOM|SEASON/) {
            $pfields[0] =~ /^(\d):M$/;
            my $freq_m = $1;
            $pfields[1] =~ /^(\d+):D$/;
            my $on = $1;
            return 0 unless exists $periods_mo{$freq_m}; # not handled
            $field->update(
                w => $periods_mo{$freq_m}, y => sprintf('pd%02d', $on)
            );
            return 1; # early out
        }
        elsif ($pfields[0] =~ /^[12]:W$/ and $pfields[1] =~ /^\d+:D$/) {
            $pfields[0] =~ /^([12]):W$/;
            my $freq_w = $1;
            $pfields[1] =~ /^(\d+):D$/;
            my $on = $1;
            $field->update(
                w => ($freq_m == 1 ? 'w' : 'e'), y => sprintf('pd%s', $dow[$on-1])
            );
            return 1; # early out
        }
        elsif ($pfields[0] =~ /^[1-3]:Y$/ and $pfields[1] =~ /^\d+:D\.\d+:M$/) {
            $pfields[0] =~ /^([1-3]):Y$/;
            my $freq_y = $1;
            $pfields[1] =~ /^(\d+):D\.(\d+):M$/;
            my $on_day = $1;
            my $of_mo = $2;
            $field->update(
                w => $yf[$freq_y],
                y => sprintf('pd%02d%02d', $of_mo, $on_day)
            );
            return 1; # early out
        }
    } elsif (@pfields == 3) {
        if ($pfields[0] =~ /^\d:M$/ && $pfields[1] =~ /^\d+:D(;|$)/) {
            $pfields[0] =~ /^(\d):M$/;
            my $months = $1;

            my @dates;
            foreach my $date (split /;/, $pfields[1]) {
                $date =~ /^(\d+):D/ or return 0; # not handled
                push @dates, sprintf("%02d", $1);
            }
            my $potential = 12 / $months * @dates;
            my @combos = split /;/, $pfields[2];
            my @cparts, @oparts;
            foreach my $combo (@combos) {
                if ($combo =~ /^(\d+):M\.0:Y/) {
                    my $squash = $1;
                    push @cparts, sprintf("%02d/%02d", $squash - 1, $squash);
                } elsif ($combo =~ /(\d+):D\.(\d+):M\.0:Y/) {
                    push @oparts, sprintf("%02d%02d", $2, $1);
                } else {
                    return 0; # abort, not handled yet
                }
            }
            my $u;
            $u = $potential - @cparts if @cparts;
            $u = $potential - @oparts if @oparts;

            my $w;
            if ($months == 1) {
                $w = $periods_mo{$months};
                $w = 's' if $u > 12;
            } elsif (exists $periods_mo{$months}) {
                $w = $periods_mo{$months};
            } else {
                return 0; # abort, not handled
            }
            $field->update(u => $u, v => 'r', w => $w);
            $field->add_subfields(y => 'cm' . join(",", @cparts)) if @cparts;
            $field->add_subfields(y => 'od' . join(",", @oparts)) if @oparts;
            $field->add_subfields(y => 'pd' . join(",", @dates));
            return 1; # early out
        }
    }

    return 0; # fail
}

sub record_bits_to_scap {
    my ($record) = @_;

    # set up constants
    my $field = new MARC::Field(
        '853',  # tag doesn't really matter here
        2 => '0',
        8 => '1',
        i => '(year)'
    );

    # when nam_type is true, use sub_iss for numeric $w
    $field->update(a => $record->{SERC_LBL1}) if $record->{SERC_LBL1};
    if ($record->{SERC_LBL2}) {
        $field->update(b => $record->{SERC_LBL2});
        $field->update(u => $record->{SERC_LMT2} || 'var');
        $field->update(v => 'r');
    }

    # return the representation we need
    return (new JSON::XS)->encode([
        $field->indicator(1),
        $field->indicator(2),
        map { @$_ } $field->subfields
    ]) if pubcycle_to_scap($record, $field);

    return "null";
}

# actually parses serctl.data, the file with all the DOCUMENT BOUNDARY and
# other stuff
sub load_serctl_export {
    my ($filename) = @_;

    open FH, "<$filename" or die ("can't read $filename: $!");

    my $entries = [];
    my $entry;
    while (<FH>) {
        chomp;

        # If we don't match this regex, move to next entry.
        if (not /^\.(\w+)\.(?:.+\|a(.+))?$/) {
            push @$entries, $entry if $entry;
            $entry = {};
            next;
        }

        # If we don't have a defined $2, just move to next line.
        next unless defined $2; 

        $entry->{$1} = $2;
    }
    close FH;

    return $entries;
}

sub unique_keys {
    my ($data) = @_;

    my $small = {};
    foreach my $hash (@$data) {
        foreach (keys %$hash) {
            $small->{$_} ||= 0;
            $small->{$_}++;
        }
    }

    return $small;
}

# The title key map is a simple text file made up of lines. Each line contains
# two tokens separated by a space.  The first one is the value of
# SERC_TITLE_KEY.  The second one is the id of the biblio.record_entry row
# that corresponds to it.  I don't have a good script for making that yet.
# SERC_TITLE_KEY can be different things (ISxN, 035‡a, etc) so you have to
# build the map by using various database queries (and hand de-duping).

sub load_title_key_map {
    my ($filename) = @_;

    open FH, "<$filename" or die "$filename: $!";

    my $map = {};
    while (<FH>) {
        /^(\S+) (\d+)/ or next;
        $map->{$1} = $2;
    }

    close FH;

    return $map;
}

############################# MAIN ###############################

my $opts = { 
    "i" => "-",     # input file
    "p" => "dump",  # operation
    "t" => undef
};
my $operations = {
    "dump" => sub { # Just parse input file (serctl.data) and dump resulting
                    # data structure
        my ($opts) = @_;
        print Dumper(load_serctl_export($opts->{i})), "\n";

        return 0;
    },
    "keys" => sub { # Over the whole list of records, how often do all keys
                    # appear?  This is an analysis tool.
        my ($opts) = @_;
        my $freq = unique_keys(load_serctl_export($opts->{i}));

        my @keys = reverse sort { $freq->{$a} <=> $freq->{$b} } keys %$freq;

        foreach (@keys) {
            printf ("%-19s %d\n", $_, $freq->{$_});
        }
        return 0;
    },
    "map" => sub {  # Combine records from input file with title key map and
                    # perform transformations suitable for evergreen import.
        my ($opts) = @_;

        my $title_key_map = load_title_key_map($opts->{t});
        my $data = load_serctl_export($opts->{i});

        foreach my $record (@$data) {
            my $title_key = $record->{SERC_TITLE_KEY};
            if (!($record->{bre} = $title_key_map->{$title_key})) {
                $record->{unready} = 1;
                next;
            }

            $record->{number_of_streams} = $record->{SERC_REC_COP};
            $record->{subscription_owning_lib} = $record->{SERC_LIB};
            $record->{distribtion_holding_lib} = substr($record->{HOLDING_CODE}, 0, 1);

            $record->{scap_active} =
                $record->{SERC_STATUS} eq 'ACTIVE' ? 't' : 'f';
            $record->{scap_pattern_code} = record_bits_to_scap($record) if $record->{PUBCYCLE_DEF};

            # save uppercase keys' values in a note
            my @uc_keys = grep { $_ !~ /[a-z]/ } (keys %$record);
            $record->{note_text} = join(
                "\n",
                map { "$_: $record->{$_}" } @uc_keys
            );

            # and remove uppercase keys now
            delete $record->{$_} for @uc_keys;
        }

        if ($opts->{u}) {
            my $unready = [grep { $_->{unready} } @$data];
            if ($unready) {
                open FH, ">$opts->{u}" or die "$opts->{u}: $!";
                print FH Dumper($unready), "\n";
                close FH;
            }
        }

        print Dumper($data), "\n";

        return 0;
    }
};

getopts("i:p:t:u:", $opts) or die("usage: $0\n\t[-i infile]\n\t[-p operation]\n\t[-t title_key_map]\n\t[-u dump_file_for_unmapped_records]");


my $operation = lc $opts->{p};
exit $operations->{$operation}->($opts) if exists $operations->{$operation};
die (
    "specify a valid operation\n$0 -p [" .
    join(" | ", (keys %$operations)) . "]"
);
