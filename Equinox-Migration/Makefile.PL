use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Equinox::Migration',
    AUTHOR              => 'Shawn Boyette <sboyette@esilibrary.com>',
    VERSION_FROM        => 'lib/Equinox/Migration.pm',
    ABSTRACT_FROM       => 'lib/Equinox/Migration.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Equinox-Migration-*' },
);
