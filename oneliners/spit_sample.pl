#!/usr/bin/perl
my @lines = <>;

foreach my $i ( 1..20 ) {
    $length = scalar( @lines );
    $idx = int rand ($length);
    print $lines[$idx];
    splice(@lines,$idx,1);
}
