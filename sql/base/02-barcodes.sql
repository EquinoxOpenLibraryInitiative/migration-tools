CREATE OR REPLACE FUNCTION migration_tools.rebarcode (o TEXT, t BIGINT) RETURNS TEXT AS $$
    DECLARE
        n TEXT := o;
    BEGIN
        IF o ~ E'^\\d+$' AND o !~ E'^0' AND length(o) < 19 THEN -- for reference, the max value for a bigint is 9223372036854775807.  May also want to consider the case where folks want to add prefixes to non-numeric barcodes
            IF o::BIGINT < t THEN
                n = o::BIGINT + t;
            END IF;
        END IF;

        RETURN n;
    END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

-- expand_barcode
--   $barcode      source barcode
--   $prefix       prefix to add to barcode, NULL = add no prefix
--   $maxlen       maximum length of barcode; default to 14 if left NULL
--   $pad          padding string to apply to left of source barcode before adding
--                 prefix and suffix; set to NULL or '' if no padding is desired
--   $suffix       suffix to add to barcode, NULL = add no suffix
--
-- Returns a new string consisting of prefix concatenated with padded barcode and suffix.
-- If new barcode would be longer than $maxlen, the original barcode is returned instead.
--
CREATE OR REPLACE FUNCTION migration_tools.expand_barcode (TEXT, TEXT, INTEGER, TEXT, TEXT) RETURNS TEXT AS $$
    my ($barcode, $prefix, $maxlen, $pad, $suffix) = @_;

    # default case
    return unless defined $barcode;

    $prefix     = '' unless defined $prefix;
    $maxlen ||= 14;
    $pad        = '0' unless defined $pad;
    $suffix     = '' unless defined $suffix;

    # bail out if adding prefix and suffix would bring new barcode over max length
    return $barcode if (length($prefix) + length($barcode) + length($suffix)) > $maxlen;

    my $new_barcode = $barcode;
    if ($pad ne '') {
        my $pad_length = $maxlen - length($prefix) - length($suffix);
        if (length($barcode) < $pad_length) {
            # assuming we always want padding on the left
            # also assuming that it is possible to have the pad string be longer than 1 character
            $new_barcode = substr($pad x ($pad_length - length($barcode)), 0, $pad_length - length($barcode)) . $new_barcode;
        }
    }

    # bail out if adding prefix and suffix would bring new barcode over max length
    return $barcode if (length($prefix) + length($new_barcode) + length($suffix)) > $maxlen;

    return "$prefix$new_barcode$suffix";
$$ LANGUAGE PLPERLU STABLE;

-- add_codabar_checkdigit
--   $barcode      source barcode
--
-- If the source string is 13 or 14 characters long and contains only digits, adds or replaces the 14
-- character with a checkdigit computed according to the usual algorithm for library barcodes
-- using the Codabar symbology - see <http://www.makebarcode.com/specs/codabar.html>.  If the
-- input string does not meet those requirements, it is returned unchanged.
--
CREATE OR REPLACE FUNCTION migration_tools.add_codabar_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    return $barcode if $barcode !~ /^\d{13,14}$/;
    $barcode = substr($barcode, 0, 13); # ignore 14th digit
    my @digits = split //, $barcode;
    my $total = 0;
    $total += $digits[$_] foreach (1, 3, 5, 7, 9, 11);
    $total += (2 * $digits[$_] >= 10) ? (2 * $digits[$_] - 9) : (2 * $digits[$_]) foreach (0, 2, 4, 6, 8, 10, 12);
    my $remainder = $total % 10;
    my $checkdigit = ($remainder == 0) ? $remainder : 10 - $remainder;
    return $barcode . $checkdigit; 
$$ LANGUAGE PLPERLU STRICT STABLE;

-- add_code39mod43_checkdigit
--   $barcode      source barcode
--
-- If the source string is 13 or 14 characters long and contains only valid
-- Code 39 mod 43 characters, adds or replaces the 14th
-- character with a checkdigit computed according to the usual algorithm for library barcodes
-- using the Code 39 mod 43 symbology - see <http://en.wikipedia.org/wiki/Code_39#Code_39_mod_43>.  If the
-- input string does not meet those requirements, it is returned unchanged.
--
CREATE OR REPLACE FUNCTION migration_tools.add_code39mod43_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    return $barcode if $barcode !~ /^[0-9A-Z. $\/+%-]{13,14}$/;
    $barcode = substr($barcode, 0, 13); # ignore 14th character

    my @valid_chars = split //, '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-. $/+%';
    my %nums = map { $valid_chars[$_] => $_ } (0..42);

    my $total = 0;
    $total += $nums{$_} foreach split(//, $barcode);
    my $remainder = $total % 43;
    my $checkdigit = $valid_chars[$remainder];
    return $barcode . $checkdigit;
$$ LANGUAGE PLPERLU STRICT STABLE;

-- add_mod16_checkdigit
--   $barcode      source barcode
--
-- https://www.activebarcode.com/codes/checkdigit/modulo16.html

CREATE OR REPLACE FUNCTION migration_tools.add_mod16_checkdigit (TEXT) RETURNS TEXT AS $$
    my $barcode = shift;

    my @digits = split //, $barcode;
    my $total = 0;
    foreach $digit (@digits) {
        if ($digit =~ /[0-9]/) { $total += $digit;
        } elsif ($digit eq '-') { $total += 10;
        } elsif ($digit eq '$') { $total += 11;
        } elsif ($digit eq ':') { $total += 12;
        } elsif ($digit eq '/') { $total += 13;
        } elsif ($digit eq '.') { $total += 14;
        } elsif ($digit eq '+') { $total += 15;
        } elsif ($digit eq 'A') { $total += 16;
        } elsif ($digit eq 'B') { $total += 17;
        } elsif ($digit eq 'C') { $total += 18;
        } elsif ($digit eq 'D') { $total += 19;
        } else { die "invalid digit <$digit>";
        }
    }
    my $remainder = $total % 16;
    my $difference = 16 - $remainder;
    my $checkdigit;
    if ($difference < 10) { $checkdigit = $difference;
    } elsif ($difference == 10) { $checkdigit = '-';
    } elsif ($difference == 11) { $checkdigit = '$';
    } elsif ($difference == 12) { $checkdigit = ':';
    } elsif ($difference == 13) { $checkdigit = '/';
    } elsif ($difference == 14) { $checkdigit = '.';
    } elsif ($difference == 15) { $checkdigit = '+';
    } else { die "error calculating checkdigit";
    }

    return $barcode . $checkdigit;
$$ LANGUAGE PLPERLU STRICT STABLE;

