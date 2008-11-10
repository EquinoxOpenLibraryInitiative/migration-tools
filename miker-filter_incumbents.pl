#!/usr/bin/perl
use warnings;
use strict;

use Getopt::Long;
use Time::HiRes qw/time/;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );

# configuration hashref
my $conf  = ();
#initialize($conf);

my $idfile = shift;
my $marcfile = shift;
my $import = shift;
my $shelve = shift;

my %id;

open F, "<$idfile";
while (<F>) {
	chomp;
	$id{$_} = 1;
}

close F;

my $M; my $I; my $S;
open $M, '<:utf8', $marcfile;
open $I, '>:utf8', $import;
open $S, '>:utf8', $shelve;

my $starttime = time;
my $count = 0;
my $icount = 0;
my $scount = 0;
while (<$M>) {
    /tag="901" ind1=" " ind2=" ">.*?<subfield code="c">(\d+)</;
    if ( $id{$1} ) {
        print $I $_;
        $icount++;
    } else {
        print $S $_;
        $scount++;
    }
    $count++;

    unless ($count && $count % 100) {
        print STDERR "\r$count\t(shelved: $scount, import: $icount)\t". $count / (time - $starttime);
    }
}

=head2 initialize

Performs boring script initialization. Handles argument parsing,
mostly.

=cut

sub initialize {
    my ($c) = @_;
    my @missing = ();

    # set mode on existing filehandles
    binmode(STDIN, ':utf8');

    my $rc = GetOptions( $c,
                         'incoming',
                         'incumbent',
                         'incoming-tag|incot=i',
                         'incoming-subfield|incos=s',
                         'incumbent-tag|incut=i',
                         'incumbent-subfield|incus=s',
                         'output|o=s',
                         'help|h',
                       );
    show_help() unless $rc;
    show_help() if ($c->{help});

    $c->{'incoming-tag'}         = 903;
    $c->{'incoming-subfield'}    = 'a';
    $c->{'incoming-matchfile'}   = '';
    $c->{'incoming-nomatchfile'} = '';
    $c->{'incumbent-tag'}         = 901;
    $c->{'incumbent-subfield'}    = 'a';
    $c->{'incumbent-matchfile'}   = '';
    $c->{'incumbent-nomatchfile'} = '';
    my @keys = keys %{$c};
    show_help() unless (@ARGV and @keys);
    for my $key ('renumber-from', 'tag', 'subfield', 'output')
      { push @missing, $key unless $c->{$key} }
    if (@missing) {
        print "Required option: ", join(', ', @missing), " missing!\n";
        show_help();
    }

}


=head2 show_help

Display usage message when things go wrong

=cut

sub show_help {
print <<HELP;
Usage is: $0 [REQUIRED ARGS]
Req'd Arguments
  --renumber-from=N        -rf First id# of new sequence
  --tag=N                  -t  Which tag to use
  --subfield=X             -s  Which subfield to use
  --output=<file>          -o  Output filename

Any number of input files may be specified; one output file will result.
HELP
exit 1;
}
