package DBIx::JoinedColumnsAggregator;

use strict;
use warnings;
use Carp ();

use base qw(Exporter);

our @EXPORT = qw(aggregate_joined_columns);

our $VERSION = '0.01';



sub aggregate_joined_columns {
    my ( $itr, $opt ) = @_;

    Carp::croak("Set pk and refs.") unless $opt->{ pk } and $opt->{ refs };

    if ( ref( $itr ) eq 'ARRAY' ) {
        return wantarray ? () : [] unless @$itr;
        $itr = DBIx::JoinedColumnAggregator::DummyIterator->new( $itr );
    }

    my @pks        = ref $opt->{ pk } ? @{ $opt->{ pk } } : $opt->{ pk };
    my $refs       = $opt->{ refs };
    my $setter     = $opt->{ setter } || sub { $_[0]->{ $_[1] } = $_[2] };
    my $nextmethod = $opt->{ next_method } || 'next';

    my $row_object = ($opt->{ access_style } and $opt->{ access_style } eq 'hash') ? 1 : 0;
    my $getter     = ($opt->{ access_style } && ref($opt->{ access_style }) eq 'CODE')
                                                            ? $opt->{ access_style } : undef;

    my %fetched;
    my @representatives;
    my $found_values = {};

    if ( not $itr->can( $nextmethod ) ) {
        Carp::croak("Iterator must have '$nextmethod' method.");
    }

    # aggregation
    while ( my $object = $itr->$nextmethod() ) {
        my $pk = $row_object ? join( ":", map { $object->{$_} } @pks ) # set pk data
               : $getter     ? join( ":", map { $getter->( $object, $_ ) } @pks )
               :               join( ":", map { $object->$_() } @pks );

        unless ( $fetched{ $pk } ) {
            $fetched{ $pk } = { object => $object, rows => {} };
            push @representatives, $fetched{ $pk };
        }

        for my $ref_name ( keys %$refs ) { # access joined data
            my @cols  = @{ $refs->{ $ref_name } };
            my @items = $row_object ? map { $_ => $object->{$_} } @cols
                      : $getter     ? map { $getter->( $object, $_ ) } @cols
                      :               map { $_ => $object->$_() } @cols;
             next if $found_values->{ $pk }->{ $ref_name }
                                    ->{ join( "\0", map { defined $_ ? $_ : '\0NULL\0' } @items ) }++;
             push @{ $fetched{ $pk }->{ rows }->{ $ref_name } }, scalar(@cols) == 1 ? $items[1] : { @items };
        }
    }

    # merging
    for my $obj_and_rows ( @representatives ) {
        my $object = $obj_and_rows->{ object };

        for my $ref_name ( keys %$refs ) {
            my $rows   = $obj_and_rows->{ rows }->{ $ref_name };

            next unless $rows;

            if ( @$rows == 1 ) { # Is joined result a null?
                my $row = $rows->[0];
                my $val = ref($row) eq 'HASH'  ?
                                        (scalar(grep { !defined } values %{ $row }) != scalar(keys %{ $row }))
                        : ref($row) eq 'ARRAY' ? defined($row)
                        : defined($row);
                $rows = [] unless $val;
            }

            $setter->( $object, $ref_name, $rows, $opt );
        }
    }

    return wantarray ? ( map { $_->{ object } } @representatives )
                     : [ map { $_->{ object } } @representatives ];
}


package
    DBIx::JoinedColumnAggregator::DummyIterator;

sub new {
    my ( $class, $list ) = @_;
    bless {
        idx   => 0,
        data  => $list,
    }, $class;
}


sub next {
    $_[0]->{ data }->[ $_[0]->{ idx }++ ];
}


1;
__END__

=pod

=head1 NAME

DBIx::JoinedColumnsAggregator - aggregating joined values

=head1 SYNOPSYS

    use DBIx::JoinedColumnsAggregator;
    
    # using DBI
    use DBI;
    
    my $dbh = DBI->connect( 'dbi:SQLite:your_data', '', '' );
    my $sth = $dbh->prepare( $a_sql_having_lef_join_closures );
    
    $sth->execute();
    
    my @data = $aggregate_joined_columns( $sth, {
        pk => ['id'],
        refs => {
            product_codes => ['product_code'],
            groups        => ['group_id', 'group_name'],
        },
        access_style => 'hash',
        next_method  => 'fetchrow_hashref',
    } );
    
    for my $data ( @data ) {
        # $data->{ product_codes } return an array referance of 'product_code'
        # $data->{ groups } return an array ref 
        #               of hash refs having 'group_id' and 'group_name'.
    }
    
    # using ORM like DBIx::Skinny or Teng or etc.
    # use and setup the module...
    # ...
    my $itr = $skinny->search_by_sql( $a_sql_having_lef_join_closures );
    my $data = $aggregate_joined_columns( $itr, {
        pk => ['id'],
        refs => {
            product_codes => ['product_code'],
            groups        => ['group_id'],
        },
        setter => sub {
            my ( $object, $tag, $rows ) = @_;
            $object->{ extra }->{ $tag } = $rows;
        },
    } );
    
    for my $data ( @{$data} ) {
        # $data->{ extra }->{ product_codes } return a list ref of 'product_code'
        # $data->{ extra }->{ groups } return a llist ref 
        #                        of hash refs having 'group_id' and 'group_name'.
    }
    

=head1 DESCRIPTION

This module provides a function which aggregates joined columns.



=head1 FUNCTION

=head2 aggregate_joined_columns

    $list = aggregate_joined_columns( $iterator, $options );
    @list = aggregate_joined_columns( $iterator, $options );
    
    $list = aggregate_joined_columns( $arrayref, $options );
    @list = aggregate_joined_columns( $arrayref, $options );

Takes an iterator or a list reference and returns a list reference (scalar context)
or a list (list context).

    # some schema
    CREATE TABLE recipes (
        id      integer,
        name    text,
        PRIMARY KEY ( id )
    );
    
    CREATE TABLE recipe_product_code_rels (
        recipe_id       integer,
        product_code    varchar(10),
        PRIMARY KEY ( recipe_id, product_code ),
        FOREIGN KEY ( recipe_id ) REFERENCES recipes (id) ON DELETE CASCADE
    );
    
    CREATE TABLE groups (
        id      integer,
        name    text,
        PRIMARY KEY ( id )
    );
    
    CREATE TABLE recipe_group_rels (
        recipe_id       integer,
        group_id        integer,
        PRIMARY KEY ( recipe_id, group_id ),
        FOREIGN KEY ( recipe_id ) REFERENCES recipes (id) ON DELETE CASCADE
        FOREIGN KEY ( group_id ) REFERENCES groups (id) ON DELETE CASCADE
    );
    
    # your program
    my $dbh = DBI->connect( ... );
    my $sth = $dbh->prepare(q{
        SELECT
            r.id, r.name, code_rel.product_code, g.id AS group_id, g.name AS group_name
        FROM recipes r
            LEFT JOIN recipe_product_code_rels code_rel ON r.id = code_rel.recipe_id
            LEFT JOIN recipe_group_rels group_rel ON r.id = group_rel.recipe_id
            LEFT JOIN groups g ON g.id = group_rel.group_id
    });
    
    $sth->execute();
    
    my $recipes = $aggregate_joined_columns( $sth, {
        pk => ['id'],
        refs => {
            product_codes => ['product_code'],
            groups        => ['group_id', 'group_name'],
        },
        access_style => 'hash',
        next_method  => 'fetchrow_hashref',
    } );
    
    for my $recipe ( @$recipes ) {
        ...
    }
    
    # $recipe->{ product_codes }
    #     => [ 'code1', 'code2', ... ]
    # 
    # $recipe->{ groups }
    #     => [
    #         { group_id => 1, group_name => 'group A' },
    #         { group_id => 2, group_name => 'group B' },
    #         { group_id => 3, group_name => 'group C' },
    #         ...
    #     ]
    # 

In the case of single column in a tag, set a list reference of simple values.
Otherwise a list reference of hash references.
If no data, set an empty list reference.

=head3 options

=over

=item pk

Primary key(s) for foreign tables.

    pk => [ 'id' ]

if there is a single primary key, the below code is acceptable too.

    pk => 'id'

=item refs

A hash reference of tags for grouping and columns .

    refs => {
        $group_tag1 => [ @tag1_columns ],
        $group_tag2 => [ @tag2_columns ],
    }

=item access_style

'hash' or 'method' or a code reference are acceptable.

'method' by default.

=item setter

A subroutine reference that sets aggregated values to objects.
Four arguments are passed into it.
First is an object expected to have aggreaged vlues.
Second is a group tag set by C<refs> option.
Third is an aggregated values.
Last is an option hash reference passed to aggregate_joined_columns.

    setter => sub {
        my ( $object, $tag, $rows, $options ) = @_;
        ...
    }

Default setter:

    sub {
        my ( $object, $tag, $rows ) = @_;
        $object->{ $tag } = $rows;
    }


=item next_method

Calling this method against the iterator in aggregate_joined_columns.
C<next> by default.

If you pass a list reference into aggregate_joined_columns instead of an iterator,
you should not set C<netx_method>. Because the list reference is wrapped by
a dummy iterator class using C<next> method in that case.

=back


=head1 EXPORT

C<aggregate_joined_columns> is exported by default.

=head1 SEE ALSO

L<DBI>,
L<DBIx::Skinny>,
L<Teng>

=head1 AUTHOR

Makamaka Hannyaharamitu, E<lt>makamaka[at]cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2011 by Makamaka Hannyaharamitu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut

