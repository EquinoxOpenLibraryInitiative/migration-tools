#!/usr/bin/perl -w

my $count = 0;
my %records = ();

sub print_line {
    print join("\t",
        $records{ $count }{'FORM'} || '',
        $records{ $count }{'USER_ID'} || '',
        $records{ $count }{'ITEM_ID'} || '',
        $records{ $count }{'BILL_LIBRARY'} || '',
        $records{ $count }{'BILL_DB'} || '',
        $records{ $count }{'BILL_AMOUNT'} || '',
        $records{ $count }{'BILL_REASON'} || '',
    ) . "\n"; 
}

print "FORM\tUSER_ID\tITEM_ID\tBILL_LIBRARY\tBILL_DB\tBILL_AMOUNT\tBILL_REASON\n";

while (my $line = <>) {
    chomp $line; $line =~ s/[\r\n]//g;
    if ($line =~ /DOCUMENT BOUNDARY/) {
        if (defined $records{ $count }) {
            print_line();
        }
        $count++; $records{ $count } = {};
    }
    if ($line =~ /FORM=(.+)/) {
        $records{ $count }{'FORM'} = $1;
    }
    if ($line =~ /\.USER_ID\..+\|a(.+)/) {
        $records{ $count }{'USER_ID'} = $1;
    }
    if ($line =~ /\.ITEM_ID\..+\|a(.+)/) {
        $records{ $count }{'ITEM_ID'} = $1;
    }
    if ($line =~ /\.BILL_LIBRARY\..+\|a(.+)/) {
        $records{ $count }{'BILL_LIBRARY'} = $1;
    }
    if ($line =~ /\.BILL_DB\..+\|a(.+)/) {
        $records{ $count }{'BILL_DB'} = $1;
    }
    if ($line =~ /\.BILL_AMOUNT\..+\|a(.+)/) {
        $records{ $count }{'BILL_AMOUNT'} = $1;
    }
    if ($line =~ /\.BILL_REASON\..+\|a(.+)/) {
        $records{ $count }{'BILL_REASON'} = $1;
    }
}
print_line();

