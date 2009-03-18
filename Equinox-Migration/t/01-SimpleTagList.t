#!perl -T

use Test::More tests => 33;
use Equinox::Migration::SimpleTagList;

# baseline object creation
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

# range addition, as_hashref, as_listref
$stl->add_range("198..201");
is_deeply ($stl->as_hashref, { 89 => 1, 198 => 1, 199 => 1, 200 => 1, 201 => 1 });
is_deeply ($stl->as_listref, [ 89, 198, 199, 200, 201 ]);
$stl->add_range("008..011");
is_deeply ($stl->as_listref, [ 8, 9, 10, 11, 89, 198, 199, 200, 201 ]);

# creation with file
$stl = Equinox::Migration::SimpleTagList->new( file => "./t/corpus/stl-0.txt");
is ($stl->has(11), 1);
is ($stl->has('011'), 1);
is ($stl->has(12), 1);
is ($stl->has('012'), 1);
is ($stl->has(241), 1);
is ($stl->has(359), 1);
is ($stl->has(652), 1);
is ($stl->has(654), 1);
is ($stl->has(656), 1);
is ($stl->has(658), 1);
is ($stl->has(872), 1);
is ($stl->has(900), 1);
is ($stl->has(999), 1);
is ($stl->has(988), 1);
is ($stl->has(655), 0, 'exception');
is ($stl->has(987), 0, 'exception');
is ($stl->has(400), 0, 'not in input set');

$stl = Equinox::Migration::SimpleTagList->new( file => "./t/corpus/stl-1.txt");
is ($stl->has(258), 1);
is ($stl->has(259), 0, 'exception');
is ($stl->has(274), 1);
is ($stl->has(275), 0, 'exception');
is ($stl->has(286), 1);
is ($stl->has(285), 0, 'exception');
is ($stl->has(305), 1);
is ($stl->has(304), 0, 'exception');
