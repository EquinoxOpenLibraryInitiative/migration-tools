#!/usr/bin/perl

my $dataset = $ARGV[0];

my $match_to = $ARGV[1];
my $match_these = $ARGV[2];
my $match_to_score = $ARGV[3];
my $match_these_score = $ARGV[4];

print "match_to: $match_to match_these: $match_these\n";

my %pines;
my %incoming;
my %match;
my %candidate_match;
my %score;

open FILE, $match_to;
while (my $line = <FILE>) {
    chomp $line;
    my @fields = split(/\t/,$line);
    my $id = shift @fields;
    my $fp = join '^', @fields;
    if (! defined $pines{ $fp }) { $pines{ $fp } = []; }
    push @{ $pines{ $fp } }, $id;
}
close FILE;

open FILE, $match_these;
while (my $line = <FILE>) {
    chomp $line;
    my @fields = split(/\t/,$line);
    my $id = shift @fields;
    my $fp = join '^', @fields;
    if (! defined $incoming{ $fp }) { $incoming{ $fp } = []; }
    push @{ $incoming{ $fp } }, $id;
}
close FILE;

foreach my $file ( $match_to_score, $match_from_score ) {
    open FILE, $file;
    while (my $line = <FILE>) {
        chomp $line;
        my @fields = split(/\|/,$line);
        my $id = shift @fields; $id =~ s/\D//g;
        my $holdings = shift @fields; $holdings =~ s/\D//g;
        my $subtitle = shift @fields; $subtitle =~ s/^\s+//; $subtitle =~ s/\s+$//;
        $score{ $id } = [ $holdings, $subtitle ];
    }
    close FILE;
}

open RECORD_IDS, ">match.record_ids";
foreach my $fp ( keys %incoming ) {

    if (defined $pines{ $fp }) { # match!
        foreach my $id ( @{ $incoming{ $fp } } ) {
            print RECORD_IDS "$id\n";
            if ( ! defined $candidate_match{ $id } )
              { $candidate_match{ $id } = []; }
            push @{ $candidate_match{ $id } }, $fp;
        }
    }
}
close RECORD_IDS;

foreach my $id ( keys %candidate_match ) {
    my $subtitle;
    if (defined $score{ $id })
      { $subtitle = $score{ $id }[1]; }

    my @fps = @{ $candidate_match{ $id } };
    my @candidate_pines = ();

    my $subtitle_matched = 0;
    my $highest_holdings = 0;
    my $best_pines_id;

    foreach my $fp ( @fps ) {
        foreach my $pines_id ( @{ $pines{ $fp } } )  {
            my $pines_subtitle;
            if (defined $score{ $pines_id })
              { $pines_subtitle = $score{ $pines_id }[1]; }
            my $pines_holdings;
            if (defined $score{ $pines_id })
              { $pines_holdings = $score{ $pines_id }[0]; }
            if ($pines_subtitle eq $subtitle) {
                if (! $subtitle_matched) {
                    $subtitle_matched = 1;
                    $best_pines_id = $pines_id;
                    $highest_holdings = -1;
                }
            } else {
                if ($subtitle_matched) { next; }
            }
            if ( $pines_holdings > $highest_holdings ) {
                $highest_holdings = $pines_holdings;
                $best_pines_id = $pines_id;
            }
        }
    }
    print RECORD_IDS "$best_pines_id\n";
    if (! defined $match{ $best_pines_id } )
      { $match{ $best_pines_id } = [ $best_pines_id ]; }
    push @{ $match{ $best_pines_id } }, $id;
}



open GROUPINGS, ">match.groupings";
foreach my $k ( keys %match ) {
    print GROUPINGS join("^",
                         "checking",
                         $dataset,
                         $match{ $k }[0],
                         join(",",@{ $match{ $k } }),
                         join(",",@{ $match{ $k } })
                        ) . "\n";

}
close GROUPINGS;


