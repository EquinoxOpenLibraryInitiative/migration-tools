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

Using a file as input, E::M::STL generates a set of MARC datafield
tags and provides several access mechanisms to that set.

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

Takes one optional argument, C<file>. If this is speficied, the tag
list will be populated as per that file on instantiation.

Returns a E::M::STL object.

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



=head2 has

Passed a data field tag, returns 1 if that tag is in the list and 0 if
it is not.

When specifying tags under 100, they must be quoted if you wish to
include the leading zeroes

    $stl->has('011'); # is equivalent to
    $stl->has(11);

or Perl will think you're passing a (possibly malformed) octal value.

=cut

sub has { my ($self, $t) = @_; $t =~ s/^0+//; return (defined $self->{tags}{$t}) ? 1 : 0 }

=head2 as_hashref

Returns a hashref of the entire, assembled tag list.

=cut

sub as_hashref { my ($self) = @_; return $self->{tags} }

=head2 as_hashref

Returns a listref of the entire, assembled tag list (sorted
numerically by tag).

=cut

sub as_listref { my ($self) = @_; return [ sort {$a <=> $b} keys %{$self->{tags}} ] }

sub generate {
    my ($self) = @_;

    open TAGFILE, '<', $self->{conf}{file};
    while (<TAGFILE>) {
        $self->{conf}{lastwasrange} = 0;
        $self->{conf}{range}{high}  = 0;
        $self->{conf}{range}{low}   = 0;

        my @chunks = split /\s+/;
        while (my $chunk = shift @chunks) {

            # single values
            if ($chunk =~ /^\d{1,3}$/) {
                $self->add_tag($chunk);
                $self->{conf}{except} = 0;
                next;
            }

            # ranges
            if ($chunk =~ /^\d{1,3}\.\.\d{1,3}$/) {
                $self->add_range($chunk);
                $self->{conf}{except} = 0;
                next;
            }

            # 'except'
            if ($chunk eq 'except') {
                die "Keyword 'except' can only follow a range (line $.)\n"
                  unless $self->{conf}{lastwasrange};
                $self->{conf}{except} = 1;
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
    $low =~ s/^0+//;
    $high =~ s/^0+//;

    die "Ranges must be 'low..high' ($low is greater than $high)\n"
      if ($low > $high);
    if ($self->{conf}{except}) {
        die "Exception ranges must be within last addition range ($low..$high)\n"
          if ($low < $self->{conf}{range}{low} or $high > $self->{conf}{range}{high});
    }
    for my $tag ($low..$high) {
        $self->add_tag($tag)
    }

    unless ($self->{conf}{except}) {
        $self->{conf}{range}{high} = $high;
        $self->{conf}{range}{low}  = $low;
    }
    $self->{conf}{lastwasrange} = 1;
}

=head2 add_tag

=cut

sub add_tag {
    my ($self, $tag) = @_;
    $tag =~ s/^0+//;

    die "Values must be valid tags (0-999)\n"
      unless ($tag >= 0 and $tag <= 999);

    if ($self->{conf}{except}) {
        $self->remove_tag($tag);
    } else {
        die "Tag '$tag' specified twice\n"
          if $self->{tags}{$tag};
        $self->{tags}{$tag} = 1;
        $self->{conf}{lastwasrange} = 0;
    }
}

=head2 remove_tag

=cut

sub remove_tag {
    my ($self, $tag) = @_;
    $tag =~ s/^0+//;

    die "Tag '$tag' isn't in the list\n"
      unless $self->{tags}{$tag};
    delete $self->{tags}{$tag};
}

=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::SimpleTagList


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::SimpleTagList
