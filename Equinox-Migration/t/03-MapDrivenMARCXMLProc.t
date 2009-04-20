#!perl -T

#use Test::More tests => 39;
use Test::More qw(no_plan);
use Equinox::Migration::MapDrivenMARCXMLProc;

# fails
eval { my $mp =
         Equinox::Migration::MapDrivenMARCXMLProc->new(marcfile => 't/corpus/mdmp-0.txt') };
is ($@, "Argument 'mapfile' must be specified\n", 'no mapfile');

eval { my $mp =
         Equinox::Migration::MapDrivenMARCXMLProc->new(mapfile => 't/corpus/mdmpmap-00.txt') };
is ($@, "Argument 'marcfile' must be specified\n", 'no marcfile');

eval { my $mp = Equinox::Migration::MapDrivenMARCXMLProc->new };
is ($@, "Argument 'mapfile' must be specified\n", 'no mapfile');


# baseline object creation
my $mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                        mapfile  => 't/corpus/mdmpmap-00.txt',
                                                      );
is(ref $mp, "Equinox::Migration::MapDrivenMARCXMLProc", "self is self");

# parsing
my $rec = $mp->parse_record;
is (defined $rec, 1);
