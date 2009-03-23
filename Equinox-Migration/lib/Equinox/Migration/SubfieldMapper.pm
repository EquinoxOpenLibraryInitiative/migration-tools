package Equinox::Migration::SubfieldMapper;

use warnings;
use strict;

=head1 NAME

Equinox::Migration::SubfieldMapper - Generate named-field to MARC tag map from file

=head1 VERSION

Version 1.000

=cut

our $VERSION = '1.000';


=head1 SYNOPSIS

Using a file as input, E::M::SM generates a mapping of MARC subfields
to arbitrary field names, and provides several access mechanisms to
that set.

    use Equinox::Migration::SubfieldMapper;
    
    my $stl = Equinox::Migration::SubfieldMapper->new( file => ".txt" );
    my $tags = $stl->as_hashref;

or

    my $stl = Equinox::Migration::SubfieldMapper->new( file => ".txt" );
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

    my $self = bless { conf   => { mods => { multi => 1, biblevel => 1} },
                       fields => {},
                       tags   => {} }, $class;

    if ($args{file}) {
        if (-r $args{file}) {
            $self->{conf}{file} = $args{file};
            $self->generate;
        } else {
            die "Can't open  file: $!\n";
        }
    }

    return $self;
}


=head2 generate

=cut

sub has {
    my ($self, @chunks) = @_;
    return undef unless (defined $chunks[0]);

    if ($chunks[0] =~ /^\d/) {
        if (defined $chunks[1]) {
            return 1 if ( defined $self->{tags}{$chunks[0]}{$chunks[1]} );
            return 0;
        } else {
            return 1 if ( defined $self->{tags}{$chunks[0]} );
            return 0;
        }
    } else {
        if (defined $chunks[2]) {
            return 1 if ( $self->{fields}{$chunks[0]}{tag} eq $chunks[1] and
                          $self->{fields}{$chunks[0]}{sub} eq $chunks[2] );
            return undef;
        } elsif (defined $chunks[1]) {
            return 1 if ( $self->{fields}{$chunks[0]}{tag} eq $chunks[1] );
            return undef;
        } else {
            return 1 if ( defined $self->{fields}{$chunks[0]} );
            return undef;
        }
    }
}


sub generate {
    my ($self, $file) = @_;

    open TAGFILE, '<', $self->{conf}{file};
    while (<TAGFILE>) {
        next if m/^#/;
        next if m/^\s*\n$/;

        chomp;
        my @tokens = split /\s+/;

        if (defined $tokens[3]) {
            $self->add( field => $tokens[0], tag => $tokens[1],
                        sub   => $tokens[2], mod => $tokens[3] );
        } else {
            $self->add( field => $tokens[0], tag => $tokens[1], sub => $tokens[2] );
        }
    }

}

sub add {
    my ($self, %toks) = @_;

    # check bits for validity
    $self->validate(\%toks);

    $toks{mod} = (defined $toks{mod}) ? $toks{mod} : 0;

    $self->{fields}{$toks{field}} = { tag => $toks{tag}, sub => $toks{sub}, mod => $toks{mod}};
    $self->{tags}{$toks{tag}}{$toks{sub}} = $toks{field};
}

sub validate {
    my ($self, $toks) = @_;

    $.= 1 unless defined $.;

    die "Required field missing (line $.)\n"
      unless (defined $toks->{field} and defined $toks->{tag} and defined $toks->{sub});

    die "Fieldnames must start with letter (line $.)\n"
     unless ($toks->{field} =~ /^\w/);

    die "Invalid tag (line $.)\n"
      if ($toks->{tag} =~ /\D/ or $toks->{tag} < 0 or $toks->{tag} > 999);

    die "Invalid subfield code (line $.)\n"
      if (length $toks->{sub} != 1 or $toks->{sub} =~ /[^a-z0-9]/);

    # the next thing (if it exists), must be a comment or valid modifier
    if (defined $toks->{mod}) {
        die "Unknown chunk (line $.)\n"
          unless (defined $self->{conf}{mods}{$toks->{mod}} or $toks->{mod} =~ /^#/);
    }

    die "Fieldnames must be unique (line $.)\n"
      if (defined $self->{fields}{$toks->{field}});

    die "Subfields cannot be multimapped (line $.)\n"
      if (defined $self->{tags}{$toks->{tag}}{$toks->{sub}});
}


=head1 AUTHOR

Shawn Boyette, C<< <sboyette at esilibrary.com> >>

=head1 BUGS

Please report any bugs or feature requests to the above email address.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Equinox::Migration::SubfieldMapper


=head1 COPYRIGHT & LICENSE

Copyright 2009 Equinox, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Equinox::Migration::SimpleTagList
