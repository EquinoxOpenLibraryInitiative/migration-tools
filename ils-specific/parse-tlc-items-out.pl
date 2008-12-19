#!/usr/bin/perl

# This parses TCL items-out reports converted from excel to csv, turning them
# into a tab separated file.  arg!

my $state;
my $patron;
my $item;
my $out;
my $due;
my $price;

my ($a,$b,$c,$d,$e,$f,$g) = (0,1,2,3,4,5,6,7);

print "patron\titem\tout\tdue\tprice\n";

while (<>) {
    chomp;
    my @fields = split /\t/;

    if ( (!$state || $state eq 'item' || $state eq 'none') && $fields[$f] eq 'Borrower ID') {
        $state = 'borrower';
        next;
    }

    if ($state eq 'borrower') {
        $patron = $fields[$f];
        $state = 'none';
        next;
    }

    if ($state eq 'none' && $fields[$b] eq 'Item ID') {
        $state = 'item';
        next;
    }

    if ($state eq 'item' && $fields[$b] =~ /^\d+$/o) {
        $item = $fields[$b];
        if ($fields[$f] =~ /^(\d+)\/(\d+)\/(\d+)$/) {
            $out = sprintf('%04d-%02d-%02d', 2000 + $3, $1, $2);
        }
        if ($fields[$e] =~ /^(\d+)\/(\d+)\/(\d+)$/) {
            $due = sprintf('%04d-%02d-%02d', 2000 + $3, $1, $2);
        }
        ($price = $fields[$g]) =~ s/\*//go;
        print join("\t", $patron, $item, $out, $due, $price) . "\n";
    }
}
