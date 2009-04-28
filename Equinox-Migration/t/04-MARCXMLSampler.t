#!perl -T

#use Test::More tests => 39;
use Test::More qw(no_plan);
use Equinox::Migration::MARCXMLSampler;

# fails
eval { my $mp =
         Equinox::Migration::MARCXMLSampler->new(tagfile => 't/corpus/mdmpmap-00.txt') };
is ($@, "Argument 'marcfile' must be specified\n", 'no marcfile');


# baseline object creation
my $mp = Equinox::Migration::MARCXMLSampler->new( marcfile  => 't/corpus/mdmp-0.txt');
is(ref $mp, "Equinox::Migration::MARCXMLSampler", "self is self");

# simple, original sample tests inherited from MDMP
$mp = Equinox::Migration::MARCXMLSampler->new( marcfile  => 't/corpus/mdmp-0.txt',
                                               mapstring => '999',
                                             );
$mp->parse_records;
my $sample = $mp->{data}{samp};
is (defined $sample->{999}, 1);
is (defined $sample->{999}{x}, 1);
is ($sample->{999}{x}{value}, 'MYSTERY', 'Should be the first seen value');
is ($sample->{999}{x}{count}, 7, 'One real in each record, plus 3 synthetic in last rec');
is ($sample->{999}{x}{rcnt}, 4, 'Occurs in all records');
is ($sample->{999}{s}{rcnt}, 3, 'Was removed from one record');

my $tags = $mp->{data}{tags};
is ($tags->{961}, 4);
is ($tags->{250}, 1);
