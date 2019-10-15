#!/usr/bin/perl -w
###############################################################################
=pod

=head1 NAME

measurespeed - program for calculating units per second between the current invocation
and the previous invocation

=head1 SYNOPSIS

B<measurespeed> [argument] [...]

=head1 DESCRIPTION

B<measurespeed> is used to measure the change in number of units over a span of time.

=head1 OVERVIEW

Using B<measurespeed> should go something like this:

=over 15

=item wc -l growing_file.txt | measurespeed --reset

=item wc -l growing_file.txt | measurespeed

=item wc -l growing_file.txt | measurespeed --max=10000 # we expect for the numbers being given to stop at 10000

=back

--reset (or --first) makes measurespeed forget about previous invocations.

measurespeed will track the elapsed time between invocations and difference the current number being fed to it with the previous number.

This data is stored in .measurespeed in the current working directory

--max (or --expect) will cause measurespeed to show a progressmeter in addition to its normal calculations

--debug shows extra information for sanity checking

=head1 EXAMPLE

=over 15

=item echo 1 | measurespeed --first

=item # 10 seconds elapse

=item echo 21 | measurespeed

=back

measurespeed will calculate that 20 units have occurred in 10 seconds, and report a speed of 2 units per second.

=cut

###############################################################################

use strict;
use Pod::Usage;
use Getopt::Long;
use Date::Calc qw(Date_to_Time Today_and_Now Delta_DHMS Time_to_Date);
use Storable qw(store retrieve);

my $help;
my $reset;
my $debug;
my $max;

GetOptions(
	'max|expect=s' => \$max,
	'reset|first' => \$reset,
    'debug' => \$debug,
	'help|?' => \$help
);
pod2usage(-verbose => 2) if $help; 

my $persist = {};
if (!$reset) {
    eval {
        $persist = retrieve('.measurespeed');
        print "Previous unit = $persist->{unit}\n" if $debug;
        print "Previous time = $persist->{time}\n" if $debug;
    };
    warn $@ if $@;
}

my $current_unit;
my $line = <>;
if ($line =~ /(\d+)/) {
    $current_unit = $1;
    print "Current unit = $current_unit\n" if $debug;
}
pod2usage(-verbose => 2) unless defined $current_unit;

my $current_time = Date_to_Time(Today_and_Now());
print "Current time = $current_time\n" if $debug;

my $unit_delta;
my $time_delta;

if (!$reset && defined $persist->{time} && defined $persist->{unit}) {
    if ($persist->{unit} <= $current_unit) {
        $unit_delta = $current_unit - $persist->{unit};
        $time_delta = $current_time - $persist->{time};
        print "unit_delta = $unit_delta, time_delta = $time_delta\n" if $debug;
        my $speed = $unit_delta / $time_delta;
        print "$speed units per second\n";
        if ($max) {
            use Term::ProgressBar;
             
            my $progress = Term::ProgressBar->new ({count => $max});
            $progress->update($current_unit);

            my $eta = ($max - $current_unit) / $speed;
            print "Estimated Time Remaining: $eta seconds\n";

            use DateTime;
            my $eta_date = DateTime->from_epoch(epoch => $current_time + $eta);
            print "                          " . $eta_date->iso8601() . "\n";
        }
    } else {
        print "Current unit is less than previous unit.  Implied --reset\n";
    }
}

$persist->{unit} = $current_unit;
$persist->{time} = $current_time;
eval { store($persist, '.measurespeed'); print "Saved .measurespeed\n" if $debug; };
warn $@ if $@;

