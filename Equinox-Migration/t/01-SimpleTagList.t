#!perl -T

use Test::More tests => 49;
#use Test::More qw(no_plan);

use Equinox::Migration::SimpleTagList;

# baseline object creation
eval { my $stl = Equinox::Migration::SimpleTagList->new( file => "thefileisalie.txt" ) };
is ($@ =~ /^Can't open tags file:/, 1, 'cannot open that');

my $stl = Equinox::Migration::SimpleTagList->new();
is(ref $stl, "Equinox::Migration::SimpleTagList", "self is self");

# manual adds and removes
$stl->add_tag(89);
is ($stl->has(89), 1, 'can has tag');
is ($stl->has(904), 0, 'can not has tag');
$stl->add_tag(904);
is ($stl->has(904), 1, 'can has tag');
$stl->remove_tag(904);
is ($stl->has(904), 0, 'can not has tag');

eval { $stl->add_tag('q') };
is ($@, "Values must be numeric\n");
eval { $stl->add_tag(-37) };
is ($@, "Values must be valid tags (0-999)\n");
eval { $stl->add_tag(1027) };
is ($@, "Values must be valid tags (0-999)\n");
eval { $stl->add_tag(89) };
is ($@, "Tag '89' specified twice\n");
eval { $stl->remove_tag(11) };
is ($@, "Tag '11' isn't in the list\n");

# range addition, as_hashref, as_listref
$stl->add_range("198..201");
is_deeply ($stl->as_hashref, { 89 => 1, 198 => 1, 199 => 1, 200 => 1, 201 => 1 });
is_deeply ($stl->as_listref, [ 89, 198, 199, 200, 201 ]);
$stl->add_range("008..011");
is_deeply ($stl->as_listref, [ 8, 9, 10, 11, 89, 198, 199, 200, 201 ]);

$stl->{conf}{except} = 1;
eval { $stl->add_range("300..311") };
is ($@, "Exception ranges must be within last addition range (300..311)\n");
eval { $stl->add_range("10..311") };
is ($@, "Exception ranges must be within last addition range (10..311)\n");
eval { $stl->add_range("6..11") };
is ($@, "Exception ranges must be within last addition range (6..11)\n");
eval { $stl->add_range("17..16") };
is ($@, "Ranges must be 'low..high' (17 is greater than 16)\n");



# creation with file
$stl = Equinox::Migration::SimpleTagList->new( file => "./t/corpus/stl-0.txt");
is ($stl->has(11), 1);
is ($stl->has('011'), 1);
is ($stl->has(12), 1);
is ($stl->has('012'), 1);
is ($stl->has(241), 1);
is ($stl->has(359), 1);
is ($stl->has(400), 1);
is ($stl->has(416), 1);
is ($stl->has(652), 1);
is ($stl->has(654), 1);
is ($stl->has(656), 1);
is ($stl->has(658), 1);
is ($stl->has(872), 1);
is ($stl->has(900), 1);
is ($stl->has(999), 1);
is ($stl->has(988), 1);
is ($stl->has(411), 0, 'exception');
is ($stl->has(655), 0, 'exception');
is ($stl->has(915), 0, 'exception');
is ($stl->has(987), 0, 'exception');
is ($stl->has(500), 0, 'not in input set');

$stl = Equinox::Migration::SimpleTagList->new( file => "./t/corpus/stl-1.txt");
is ($stl->has(258), 1);
is ($stl->has(259), 0, 'exception');
is ($stl->has(274), 1);
is ($stl->has(275), 0, 'exception');
is ($stl->has(286), 1);
is ($stl->has(285), 0, 'exception');
is ($stl->has(305), 1);
is ($stl->has(304), 0, 'exception');

# file with bad token
$. = 0;
$stl = Equinox::Migration::SimpleTagList->new;
$stl->{conf}{file} = "./t/corpus/stl-2.txt";
eval {$stl->generate};
is ($@, "Unknown chunk fnord in tags file (line 1)\n");

# file with except in wrong place
$. = 0;
$stl = Equinox::Migration::SimpleTagList->new;
$stl->{conf}{file} = "./t/corpus/stl-3.txt";
eval {$stl->generate};
is ($@, "Keyword 'except' can only follow a range (line 1)\n");
