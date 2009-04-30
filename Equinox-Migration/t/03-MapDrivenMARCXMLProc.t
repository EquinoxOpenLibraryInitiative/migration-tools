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

# with map-01,  999$a and 999$q are captured. q only exists on the second
# record; the others should the placeholder value of ''
$mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                     mapfile  => 't/corpus/mdmpmap-01.txt'
                                                   );
$rec = $mp->parse_record;
is ($rec->{tags}[0]{uni}{a}, "MYS DEM", '999$a');
is ($rec->{tags}[0]{uni}{q}, "", '999$q doesnt exist here');
is ($rec->{tags}[0]{uni}{j}, undef, 'we shouldnt have captured this, even if it does exist');
$rec = $mp->parse_record;
is ($rec->{tags}[0]{uni}{a}, "MYS 2", '999$a');
is ($rec->{tags}[0]{uni}{q}, "TEST", '999$q does exist here');

# map-02 adds 999$x *not* as multi, producing a fatal error on the last record
$mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                     mapfile  => 't/corpus/mdmpmap-02.txt');
$rec = $mp->parse_record;
$rec = $mp->parse_record;
$rec = $mp->parse_record;
eval { $rec = $mp->parse_record };
is ($@, "Multiple occurances of a non-multi field: 999x at rec 4\n", '999$x not declared multi, but is');

# map-03 has 999$s as required, producing a fatal on record X
$mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                     mapfile  => 't/corpus/mdmpmap-03.txt');
eval { $rec = $mp->parse_record };
is ($@, "Required mapping 999s not found in rec 1\n", '999$s removed from this record');
eval { $rec = $mp->parse_record };
is ($@, "", '999$s exists here tho');

# map-04 has fields in 999 and 250, and multi data
$mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                     mapfile  => 't/corpus/mdmpmap-04.txt');
$rec = $mp->parse_record;
is ($rec->{tags}[0]{tag}, 250, 'should be 250');
is ($rec->{tags}[0]{uni}{a}, "1st ed.", '999$a');
is ($rec->{tags}[1]{tag}, 999, 'should be 999');
is ($rec->{tags}[1]{uni}{a}, "MYS DEM", '999$a');
is_deeply ($rec->{tags}[1]{multi}{'999x'}, ['MYSTERY'], '999$x - multi');
is_deeply ($mp->{data}{tmap}{250}, [0], 'tag map test 1a');
is_deeply ($mp->{data}{tmap}{999}, [1], 'tag map test 1b');
$rec = $mp->parse_record;
$rec = $mp->parse_record;
$rec = $mp->parse_record;
is ($rec->{tags}[0]{tag}, 999, '250 doesnt exist in this record');
is ($rec->{tags}[0]{uni}{a}, "FIC DEV", 'subfield value 4');
is_deeply ($rec->{tags}[0]{multi}{'999x'}, ['FICTION','FICTION2','FICTION3','FICTION4'],
           '999$x - multi');
is ($mp->{data}{tmap}{250}, undef, 'tag map test 2a');
is_deeply ($mp->{data}{tmap}{999}, [0], 'tag map test 2b');

# map-05 is map-04 with a "no digits" filter on 999$x
$mp = Equinox::Migration::MapDrivenMARCXMLProc->new( marcfile => 't/corpus/mdmp-0.txt',
                                                     mapfile  => 't/corpus/mdmpmap-05.txt');
is_deeply ($mp->{map}->filters('multi_field'), ['\d']);
$rec = $mp->parse_record;
$rec = $mp->parse_record;
$rec = $mp->parse_record;
$rec = $mp->parse_record;
is_deeply ($rec->{tags}[0]{multi}{'999x'}, ['FICTION'], '999$x - multi no digits');
