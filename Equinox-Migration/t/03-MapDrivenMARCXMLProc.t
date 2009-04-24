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
#
# with map-00, only the 999$a should be captured
# 903$a will *always* be captured, of course
my $rec = $mp->parse_record;
is (defined $rec, 1);
is ($rec->{egid}, 9000000, '903 captured');
is ($rec->{tags}[0]{tag}, 999, 'first (only) tag should be 999');
is ($rec->{tags}[0]{uni}{a}, "MYS DEM", 'single-ocurrance subfield "a" should be "MYS DEM"');
is ($rec->{tags}[0]{uni}{b}, undef, 'only one uni subfield defined');
is ($rec->{tags}[0]{multi},  undef, 'no multi subfields were defined');
is ($rec->{tags}[1],         undef, 'Only one tag in map');
is ($rec->{bib},             undef, 'No bib-level fields in map');
# let's go ahead and look at the rest of the file
$rec = $mp->parse_record;
is ($rec->{egid}, 9000001, '903 #2');
is ($rec->{tags}[0]{tag}, 999, 'tag id 2');
is ($rec->{tags}[0]{uni}{a}, "MYS 2", 'subfield value 2');
$rec = $mp->parse_record;
is ($rec->{egid}, 9000002, '903 #3');
is ($rec->{tags}[0]{tag}, 999, 'tag id 3');
is ($rec->{tags}[0]{uni}{a}, "FOO BAR", 'subfield value 3');
$rec = $mp->parse_record;
is ($rec->{egid}, 9000003, '903 #4');
is ($rec->{tags}[0]{tag}, 999, 'tag id 4');
is ($rec->{tags}[0]{uni}{a}, "FIC DEV", 'subfield value 4');
$rec = $mp->parse_record;
is ($rec, 0, 'no more records');
