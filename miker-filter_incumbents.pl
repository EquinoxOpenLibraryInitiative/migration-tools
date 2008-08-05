#!/usr/bin/perl

use Time::HiRes qw/time/;
use MARC::Record;
use MARC::File::XML ( BinaryEncoding => 'utf-8' );

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

my $M;
open $M, '<:utf8', $marcfile;
open $I, '>:utf8', $import;
open $S, '>:utf8', $shelve;

my $starttime = time;
my $count = 0;
my $icount = 0;
my $scount = 0;
while (<$M>) {

	/tag="901" ind1=" " ind2=" "><subfield code="a">(\d+)</;
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
		
