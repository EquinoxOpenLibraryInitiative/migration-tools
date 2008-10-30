#!/usr/bin/perl -w

my $count = 0;
my %records = ();

sub print_line {
    print join("\t",
        $records{ $count }{'FORM'} || '',
        $records{ $count }{'USER_ID'} || '',
        $records{ $count }{'ITEM_ID'} || '',
        $records{ $count }{'CHRG_LIBRARY'} || '',
        $records{ $count }{'CHRG_DC'} || '',
        $records{ $count }{'CHRG_DATEDUE'} || '',
        $records{ $count }{'CHRG_DATE_CLMRET'} || '',
    ) . "\n"; 
}

print "FORM\tUSER_ID\tITEM_ID\tCHRG_LIBRARY\tCHRG_DC\tCHRG_DATEDUE\nCHRG_DATE_CLMRET\n";

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
    if ($line =~ /\.CHRG_LIBRARY\..+\|a(.+)/) {
        $records{ $count }{'CHRG_LIBRARY'} = $1;
    }
    if ($line =~ /\.CHRG_DC\..+\|a(.+)/) {
        $records{ $count }{'CHRG_DC'} = $1;
    }
    if ($line =~ /\.CHRG_DATEDUE\..+\|a(.+)/) {
        $records{ $count }{'CHRG_DATEDUE'} = $1;
    }
    if ($line =~ /\.CHRG_DATEDUE\..+\|a(.+)/) {
        $records{ $count }{'CHRG_DATE_CLMRET'} = $1;
    }
}
print_line();

