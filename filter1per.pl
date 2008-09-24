#!/usr/bin/perl

use Time::HiRes qw/time/;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );

my $tag = shift;
my $subfield = shift;
my $idfile = shift;
my $marcfile = shift;
my $found = shift;
my $notfound = shift;
if (! ($tag && $subfield && $idfile && $marcfile && $found ) ) {
    print "filter1per.pl <tag> <subfield> <idfile> <marcfile> <output.found> [<output.notfound>]\n";
    exit 0;
}

my %id;

open F, "<$idfile";
while (<F>) {
	chomp;
	$id{$_} = 1;
}

close F;

my $M;
open $M, '<:utf8', $marcfile;
open $I, '>:utf8', $found;
if ($notfound) { open $S, '>:utf8', $notfound; }

my $starttime = time;
my $count = 0;
my $icount = 0;
my $scount = 0;
while (<$M>) {

	/tag="$tag" ind1=" " ind2=" ">.*?<subfield code="$subfield">(\d+)</;
	if ( $id{$1} ) {
		print $I $_;
		$icount++;
	} else {
        if ($notfound) {
    		print $S $_;
        }
   		$scount++;
	}
	$count++;

	unless ($count && $count % 100) {
		print STDERR "\r$count\t(notfoundd: $scount, found: $icount)\t". $count / (time - $starttime);
	}

}
print STDERR "\n";
