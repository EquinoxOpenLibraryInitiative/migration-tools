package Equinox::Migration::SubfieldMapper;

use warnings;
use strict;

=head1 NAME

Equinox::Migration::SubfieldMapper - Generate named-field to MARC tag map from file

=head1 VERSION

Version 1.002

=cut

our $VERSION = '1.002';


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


=head1 METHODS

=head2 new

Takes one optional argument, C<file>. If this is speficied, the tag
list will be populated as per that file on instantiation.

Returns a E::M::STL object.

=cut

sub new {
    my ($class, %args) = @_;

    my $self = bless { conf   => { mods => undef },
                       fields => {},
                       tags   => {} }, $class;

    if ($args{mods}) {
        die "Argument 'mods' is wrong type\n"
          unless (ref $args{mods} eq "ARRAY");
        for my $mod ( @{$args{mods}} )
          { $self->{conf}{mods}{$mod} = 1 }
    }

    if ($args{file}) {
        if (-r $args{file}) {
            $self->{conf}{file} = $args{file};
            $self->generate;
        } else {
            die "Can't open file: $!\n";
        }
    }

    return $self;
}

=head2 has

Ask it whether your mapping has various things, and it'll let you know.

    $sm->has('fieldname')      # is this fieldname mapped?
    $sm->has(901)              # are there any mappings for this tag?
    $sm->has(650,'c')          # is this tag/subfield combo mapped?
    $sm->has('name', 245, 'a') # is this name mapped to 245$a?

Returns 1 if true, 0 if false.

FIXME: use named params instead of positional

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
            return 0;
        } elsif (defined $chunks[1]) {
            return 1 if ( $self->{fields}{$chunks[0]}{tag} eq $chunks[1] );
            return 0;
        } else {
            return 1 if ( defined $self->{fields}{$chunks[0]} );
            return 0;
        }
    }
}

=head2 tags

Returns an arrayref containing the tags defined in the map.

    my $tags = $sfm->tags;
    for my tag ( @{$tags} ) {
        my $subs = $sfm->subfields($tag);
        ...
    }

=cut

sub tags {
    my ($self) = @_;
    return [ keys %{$self->{tags}} ];
}

=head2 subfields

Given a tag, return an arrayref of the subfields mapped with that tag.

    my $tags = $sfm->tags;
    for my tag ( @{$tags} ) {
        my $subs = $sfm->subfields($tag);
        ...
    }

Returns C<undef> if C<tag> is not mapped.

=cut

sub subfields {
    my ($self, $tag) = @_;
    return undef unless $self->has($tag);
    return [ keys %{$self->{tags}{$tag}} ];
}


=head2 field

Given a tag and subfield code,

    my $fname = $sm->field(945, 'p')

return the name mapped to them. Returns C<undef> if no mapping exists.

=cut

sub field {
    my ($self, $tag, $sub) = @_;
    return undef unless (defined $tag and defined $sub);
    return undef unless $self->has($tag, $sub);
    return $self->{tags}{$tag}{$sub};
}

=head2 mods

Returns the modifiers set on a mapping.

    $self->mods('fieldname')

If there are no modifiers, C<undef> will be returned. Else a hashref
will be returned.

=cut

sub mods {
    my ($self, $field) = @_;
    return undef unless $self->has($field);
    return undef unless (%{ $self->{fields}{$field}{mods} });
    return $self->{fields}{$field}{mods};
}

=head2 filters

Returns the content filters set on a mapping

    $self->filters('fieldname')

If there are no filters, C<undef> will be returned. Else a listref
will be returned.

=cut

sub filters {
    my ($self, $field) = @_;
    return undef unless $self->has($field);
    return $self->{fields}{$field}{filt};
}

=head1 MAP CONSTRUCTION METHODS

These methods are not generally accessed from user code.

=head2 generate

Generate initial mapping from file.

=cut

sub generate {
    my ($self, $file) = @_;

    open TAGFILE, '<', $self->{conf}{file};
    while (<TAGFILE>) {
        next if m/^#/;
        next if m/^\s*\n$/;

        chomp;
        my @tokens = split /\s+/;

        my $map = { mods => [], filt => [] };
        $map->{field} = shift @tokens;
        $map->{tag}   = shift @tokens;
        while (my $tok = shift @tokens) {
            last if ($tok =~ m/^#/);
            if ($tok =~ m/^[a-z]:'/) {
                $tok .= ' ' . shift @tokens
                  until ($tokens[0] =~ m/'$/);
                $tok .= ' ' . shift @tokens;
                $tok =~ s/'//;
                $tok =~ s/'$//;
            }
            if ($tok =~ m/^m:/)
              { push @{$map->{mods}}, $tok }
            elsif ($tok =~ m/^f:/)
              { push @{$map->{filt}}, $tok }
            elsif ($tok =~ m/^[a-z0-9]$/)
              { $map->{sub} = $tok }
            else
              { die "Unknown chunk '$tok' at line $.\n" }
        }
        $self->add($map);
    }
}

=head2 add

Add new item to mapping. Not usually called directly from user code.

    $sm->add( $map );

Where C<$map> is a hashref that, at a minimum, looks like

    { field => "value", tag => NNN, sub => X }

and may also have the key/value pairs

    mods => [ ITEMS ]
    filt => [ ITEMS ]

=cut

sub add {
    my ($self, $map) = @_;

    # trim the mods and filters
    my $mods = {};
    my $filt = []; my %filt = ();
    for my $m (@{$map->{mods}}) {
        die "Modifier collision '$m' at line $." if $mods->{$m};
        $m =~ s/^m://;
        $mods->{$m} = 1;
    }
    for my $f (@{$map->{filt}}) {
        die "Modifier collision '$f' at line $." if $filt{$f};
        $f =~ s/^f://;
        push @{$filt}, $f; $filt{$f} = 1;
    }
    $map->{mods} = $mods;
    $map->{filt} = $filt;

    # check bits for validity
    $self->validate($map);

    # add data to the fields hash
    $self->{fields}{ $map->{field} } = { tag => $map->{tag},
                                         sub => $map->{sub},
                                         mods => $map->{mods},
                                         filt => $map->{filt}
                                       };
    # and to the tags hash
    $self->{tags}{ $map->{tag} }{ $map->{sub} } = $map->{field};
}

=head2 validate

Passed a reference to the hash given to C<add>, validate scans its
contents for common errors and dies if there is an issue.

    * field, tag, and sub are required
    * fieldnames must start with a letter
    * fieldnames must be unique
    * tag must be between 0 and 999
    * subfield code must be a single alphanumeric character
    * tag+subfield can only be mapped once
    * if a list of allowable mod values was given in the call to
      C<new>, any modifiers must be on that list

=cut

sub validate {
    my ($self, $map) = @_;

    $.= 1 unless defined $.;

    die "Required field missing (line $.)\n"
      unless (defined $map->{field} and defined $map->{tag} and defined $map->{sub});

    die "Fieldnames must start with letter (line $.)\n"
     unless ($map->{field} =~ /^[a-zA-z]/);

    die "Invalid tag (line $.)\n"
      if ($map->{tag} =~ /[^\d\-]/ or $map->{tag} < 0 or $map->{tag} > 999);

    die "Invalid subfield code (line $.)\n"
      if (length $map->{sub} != 1 or $map->{sub} =~ /[^a-zA-Z0-9]/);

    # test mod names if we have a set to check against
    if (defined $self->{conf}{mods}) {
        for my $mod ( keys %{$map->{mods}} ) {
            die "Modifier '$mod' not allowed\n"
              unless $self->{conf}{mods}{$mod};
        }
    }

    die "Fieldnames must be unique (line $.)\n"
      if (defined $self->{fields}{$map->{field}});

    die "Subfields cannot be mapped twice (line $.)\n"
      if (defined $self->{tags}{$map->{tag}}{$map->{sub}});

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
