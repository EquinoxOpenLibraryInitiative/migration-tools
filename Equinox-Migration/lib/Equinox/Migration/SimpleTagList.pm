package Equinox::Migration::SimpleTagList;

use warnings;
use strict;

=head1 NAME

Equinox::Migration::SimpleTagList - Generate taglist from file

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Using a file as input, E::M::STL generates a set of MARC tags
(three-digit, zero-padded integers) and provides several access
mechanisms to that list.

    use Equinox::Migration::SimpleTagList;
    
    my $stl = Equinox::Migration::SimpleTagList->new( file => "trashtags.txt" );
    my $tags = $stl->as_hashref;

or

    my $stl = Equinox::Migration::SimpleTagList->new( file => "trashtags.txt" );
    if ( $stl->has($foo) ) {
        # if $foo is an element of $stl's parsed list
        # do stuff ...
    }


=head1 ROUTINES


=head2 new

Takes one argument, C<file>, which is mandatory. Returns a E::M::STL
object.

=cut

sub new {
    my ($class,%args) = @_;

    my $self = bless { conf => { except => 0,
                                 range => { high => 0, low => 0 },
                               },
                       tags => {} }, $class;

    if (-r $args{file}) {
        $self->{conf}{file} = $args{file}
    } else {
        die "Can't open tags file: $!\n";
    }

    $self->generate;
    return $self;
}



=head2 has

Passed a data field tag, returns 1 if that tag is in the list and 0 if
it is not.

=cut

sub has { my ($self, $t) = @_; return (defined $self->{tags}{$t}) ? 1 : 0 }

=head2 as_hashref

Returns a hashref of the entire, assembled tag list.

=cut

sub as_hashref { my ($self) = @_; return $self->{tags} }

=head2 as_hashref

Returns a listref of the entire, assembled tag list.

=cut

sub as_listref { my ($self) = @_; return \(keys %{$self->{tags}}) }

sub generate {
    my ($self) = @_;

    open TAGFILE, '<', $self->{conf}{file};
    while (<TAGFILE>) {
        my $lastwasrange = 0;
        $self->{conf}{range}{high} = 0;
        $self->{conf}{range}{low}  = 0;
        $self->{conf}{except} = 0;

        my @chunks = split /\s+/;
        while (my $chunk = shift @chunks) {

            # single values
            if ($chunk =~ /^\d{1,3}$/) {
                $self->add_tag($chunk);
                $lastwasrange = 0;
                next;
            }

            # ranges
            if ($chunk =~ /^\d{1,3}\.\.\d{1,3}$/) {
                my ($low, $high) = $self->add_range($chunk);
                $lastwasrange = 1;
                unless ($self->{conf}{except}) {
                    $self->{conf}{range}{high} = $high;
                    $self->{conf}{range}{low}  = $low;
                }
                next;
            }

            # 'except'
            if ($chunk eq 'except') {
                die "Keyword 'except' can only follow a range (line $.)\n"
                  unless $lastwasrange;
                die "Keyword 'except' may only occur once per line (line $.)\n"
                  if $self->{conf}{except};
                $$self->{conf}{except} = 1;
                next;
            }

            die "Unknown chunk $chunk in tags file (line $.)\n";
        }
    }
}

=head2 add_range

=cut

sub add_range {
    my ($self, $chunk) = @_;
    my ($low,$high) = split /\.\./, $chunk;
    die "Ranges must be 'low..high' ($low is greater than $high on line $.)\n"
      if ($low > $high);
    if ($self->{conf}{except}) {
        die "Exception ranges must be within last addition range (line $.)\n"
          if ($low < $self->{range}{low} or $high > $self->{range}{high});
    }
    for my $tag ($low..$high) {
        $self->add_tag($tag)
    }
    return $low, $high;
}

=head2 add_tag

=cut

sub add_tag {
    my ($self, $tag) = @_;

    die "Values must be valid tags (000-999)\n"
      unless ($tag >= 0 and $tag <= 999);

    if ($self->{conf}{except}) {
        delete $self->{tags}{$tag};
    } else {
        die "Trash tag '$tag' specified twice (line $.)\n"
          if $self->{tags}{$tag};
        $self->{tags}{$tag} = 1;
    }
}


=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 TODO

=over

=item * Remove single-except rule?

=back

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::TrashTags


=head1 COPYRIGHT & LICENSE

Copyright 2009 Shawn Boyette, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::SimpleTagList
