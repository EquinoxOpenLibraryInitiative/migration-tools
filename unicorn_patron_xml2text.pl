#!/usr/bin/perl
#
# WARNING: This doesn't handle Rob's address-database-enhanced Patron XML format, with such things as Change of Address information
#
use strict;
use warnings;

use DateTime;
use Time::HiRes qw/time/;
use XML::LibXML;

my %s_map;

my $doc = XML::LibXML->new->parse_file($ARGV[0]);

my $starttime = time;
my $count = 1;

my @base_elements = (
    "user_id",
    "user_altid",
    "user_pin",
    "user_profile",
    "user_status",
    "user_priv_granted",
    "user_priv_expires",
    "user_mailingaddr",
    "birthdate",
    "last_name",
    "first_name",
    "middle_name",
    "suffix_name",
    "note",
    "comment"
);

my @addr_elements = (
    "std_line1",
    "std_line2",
    "std_city",
    "std_state",
    "std_zip",
    "dayphone",
    "homephone",
    "workphone",
    "email"
);

print STDOUT join("\t", @base_elements);
foreach my $addr ( 1..3 ) {
    print STDOUT "\t" . join("\t", @addr_elements);
}
print STDOUT "\n";

for my $patron ( $doc->documentElement->childNodes ) {
	next if ($patron->nodeType == 3);

	my $bc = $patron->findvalue( 'user_id' );
	if (exists($s_map{$bc})) {
		$count++;
		warn "\n!!! already saw barcode $bc, skipping\n";
		next;
	} else {
		$s_map{$bc} = 1;
	}

	unless (defined($bc)) {
		my $xml = $patron->toString;
		warn "\n!!! no barcode found in UMS data, user number $count, xml => $xml \n";
		$count++;
		next;
	}

    foreach my $e ( @base_elements ) {
        my $v = $patron->findvalue( $e );
        if ( $v && ( $e eq 'birthdate' || $e eq 'user_priv_granted' || $e eq 'user_priv_expires' ) ) { $v = parse_date($v); }
        print STDOUT ( $v ? $v : '' ) . "\t";
    }

	my %addresses;

	for my $addr ( $patron->findnodes( "Address" ) ) {
		my $addr_type = $addr->getAttribute('addr_type');
		$addresses{$addr_type} = $addr;
	}

    foreach my $t ( 1..3 ) {
        if ($addresses{$t}) {
            foreach my $e ( @addr_elements ) {
                my $v = $addresses{$t}->findvalue( $e );
                print STDOUT ( $v ? $v : '' ) . "\t";
            }
        } else {
            foreach ( @addr_elements ) { print STDOUT "\t"; }
        }
    }

    print STDOUT "\n";
	$count++;
}

sub parse_date {
	my $string = shift;
	my $group = shift;

	my ($y,$m,$d);

	if ($string eq 'NEVER') {
		my (undef,undef,undef,$d,$m,$y) = localtime();
		return sprintf('%04d-%02d-%02d', $y + 1920, $m + 1, $d);
	} elsif (length($string) == 8 && $string =~ /^(\d{4})(\d{2})(\d{2})$/o) {
		($y,$m,$d) = ($1,$2,$3);
	} elsif ($string =~ /(\d+)\D(\d+)\D(\d+)/o) { #looks like it's parsable
		if ( length($3) > 2 )  { # looks like mm.dd.yyyy
			if ( $1 < 99 && $2 < 99 && $1 > 0 && $2 > 0 && $3 > 0) {
				if ($1 > 12 && $1 < 31 && $2 < 13) { # well, actually it looks like dd.mm.yyyy
					($y,$m,$d) = ($3,$2,$1);
				} elsif ($2 > 12 && $2 < 31 && $1 < 13) {
					($y,$m,$d) = ($3,$1,$2);
				}
			}
		} elsif ( length($1) > 3 ) { # format probably yyyy.mm.dd
			if ( $3 < 99 && $2 < 99 && $1 > 0 && $2 > 0 && $3 > 0) {
				if ($2 > 12 && $2 < 32 && $3 < 13) { # well, actually it looks like yyyy.dd.mm -- why, I don't konw
					($y,$m,$d) = ($1,$3,$2);
				} elsif ($3 > 12 && $3 < 31 && $2 < 13) {
					($y,$m,$d) = ($1,$2,$3);
				}
			}
		} elsif ( $1 < 99 && $2 < 99 && $3 < 99 && $1 > 0 && $2 > 0 && $3 > 0) {
			if ($3 < 7) { # probably 2000 or greater, mm.dd.yy
				$y = $3 + 2000;
				if ($1 > 12 && $1 < 32 && $2 < 13) { # well, actually it looks like dd.mm.yyyy
					($m,$d) = ($2,$1);
				} elsif ($2 > 12 && $2 < 32 && $1 < 13) {
					($m,$d) = ($1,$2);
				}
			} else { # probably before 2000, mm.dd.yy
				$y = $3 + 1900;
				if ($1 > 12 && $1 < 32 && $2 < 13) { # well, actually it looks like dd.mm.yyyy
					($m,$d) = ($2,$1);
				} elsif ($2 > 12 && $2 < 32 && $1 < 13) {
					($m,$d) = ($1,$2);
				}
			}
		}
	}

	my $date;
	if ($y && $m && $d) {
		eval {
			$date = sprintf('%04d-%02d-%-2d',$y, $m, $d)
				if (new DateTime ( year => $y, month => $m, day => $d ));
		}
	}

	return $date;
}

