package Equinox::Migration::MapDrivenXMLProc;

use warnings;
use strict;

use XML::Twig;
use Equinox::Migration::SubfieldMapper;

=head1 NAME

Equinox::Migration::MapDrivenXMLProc

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Foo

    use Equinox::Migration::MapDrivenXMLProc;


=head1 METHODS


=head2 new

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { conf => { except => 0,
                                 range  => { high => 0, low => 0 },
                                 lastwasrange => 0,
                               },
                       tags => {} }, $class;

    if ($args{file}) {
        if (-r $args{file}) {
            $self->{conf}{file} = $args{file};
            $self->generate;
        } else {
            die "Can't open tags file: $!\n";
        }
    }

    return $self;
}

=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::MapDrivenXMLProc


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::MapDrivenXMLProc
