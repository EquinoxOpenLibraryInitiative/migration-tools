#!perl -T

#use Test::More tests => 39;
use Test::More qw(no_plan);
use Equinox::Migration::MapDrivenXMLProc;

# baseline object creation
my $sm = Equinox::Migration::MapDrivenXMLProc->new();
is(ref $sm, "Equinox::Migration::MapDrivenXMLProc", "self is self");

