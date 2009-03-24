#!perl -T

#use Test::More tests => 33;
use Test::More qw(no_plan);
use Equinox::Migration::SubfieldMapper;

# baseline object creation
eval { my $sm = Equinox::Migration::SubfieldMapper->new( file => "thefileisalie.txt" ) };
is ($@ =~ /^Can't open file:/, 1, 'cannot open that');

my $sm = Equinox::Migration::SubfieldMapper->new();
is(ref $sm, "Equinox::Migration::SubfieldMapper", "self is self");

# test validation death routines
my $tokens = {};
eval { $sm->validate($tokens) };
is ($@, "Required field missing (line 1)\n", 'nothing there');
$tokens = { field => 'foo' };
eval { $sm->validate($tokens) };
is ($@, "Required field missing (line 1)\n", 'only 1 field');
$tokens = { field => 'foo', tag => 99 };
eval { $sm->validate($tokens) };
is ($@, "Required field missing (line 1)\n", 'only 2 fields');

$tokens = { field => '9wm', tag => 650, sub => 'a' };
eval { $sm->validate($tokens) };
is ($@, "Fieldnames must start with letter (line 1)\n", 'field must start with letter');

$tokens = { field => 'foo', tag => 'q', sub => 'a' };
eval { $sm->validate($tokens) };
is ($@, "Invalid tag (line 1)\n", 'nonnumeric tag');
$tokens = { field => 'foo', tag => -1, sub => 'a' };
eval { $sm->validate($tokens) };
is ($@, "Invalid tag (line 1)\n", 'tag value < 0');
$tokens = { field => 'foo', tag => 1042, sub => 'a' };
eval { $sm->validate($tokens) };
is ($@, "Invalid tag (line 1)\n", 'tag value > 999');

$tokens = { field => 'foo', tag => 650, sub => '%' };
eval { $sm->validate($tokens) };
is ($@, "Invalid subfield code (line 1)\n", 'non-alphanum subfield');
$tokens = { field => 'foo', tag => 650, sub => '' };
eval { $sm->validate($tokens) };
is ($@, "Invalid subfield code (line 1)\n", 'zero-length subfield');
$tokens = { field => 'foo', tag => 650, sub => 'qq' };
eval { $sm->validate($tokens) };
is ($@, "Invalid subfield code (line 1)\n", 'over-length subfield');

$tokens = { field => 'foo', tag => 650, sub => 'a', mod => 'bar' };
eval { $sm->validate($tokens) };
is ($@, "Unknown chunk (line 1)\n", 'Extra, non-comment content');

# and some which should have no problems
$tokens = { field => 'foo', tag => 650, sub => 'a' };
eval { $sm->validate($tokens) };
is ($@, '', 'should be fine!');
$tokens = { field => 'foo', tag => 650, sub => 'a', mod => '#', 'this', 'is', 'a', 'comment' };
eval { $sm->validate($tokens) };
is ($@, '', 'should be fine!');

# two more death: dupes
$sm->{fields}{foo} = 1;
$tokens = { field => 'foo', tag => 650, sub => 'a', mod => '#', 'this', 'is', 'a', 'comment' };
eval { $sm->validate($tokens) };
is ($@, "Fieldnames must be unique (line 1)\n", 'dupe fieldname');
$sm->{tags}{650}{a} = 1;
$tokens = { field => 'bar', tag => 650, sub => 'a', mod => '#', 'this', 'is', 'a', 'comment' };
eval { $sm->validate($tokens) };
is ($@, "Subfields cannot be multimapped (line 1)\n", 'dupe fieldname');

# test load from file
$sm = Equinox::Migration::SubfieldMapper->new( file => "./t/corpus/sm0.txt" );
is(ref $sm, "Equinox::Migration::SubfieldMapper", "self is self");
is ($sm->{tags}{949}{a}, 'call_number');
is ($sm->{tags}{999}{a}, 'call_number_alt');

# has method tests
is ($sm->has, undef, 'has nothing');
is ($sm->has(949), 1, 'has tag');
is ($sm->has(959), 0, 'has not tag');
is ($sm->has(999, 'a'), 1, 'has tag and subfield');
is ($sm->has('call_number'), 1, 'has fieldname');
is ($sm->has('call_number', 949), 1, 'has tag');
is ($sm->has('call_number', 700), 0, 'does not has tag');
is ($sm->has('call_number', 949, 'a'), 1, 'has code');
is ($sm->has('call_number', 949, 'q'), 0, 'does not has code');

# field method tests
is ($sm->{fields}{call_number}{tag}, 949);
is ($sm->{fields}{call_number}{sub}, 'a');
is ($sm->field, undef, 'null mapping is undef');
is ($sm->field(650), undef, 'half-null mapping is undef');
is ($sm->field(650,'z'), undef, 'tag+code not mapped');
is ($sm->field(949,'a'), 'call_number', 'mapping returned');

# mod method tests
is ($sm->{fields}{type}{mod}, 0);
is ($sm->{fields}{note}{mod}, 'multi');
is ($sm->mod('zzz'), undef, 'nonexistant field');
is ($sm->mod('note'), 'multi', 'multi');
