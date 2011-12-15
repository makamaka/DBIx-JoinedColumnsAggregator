package DBIx::JoinedColumnsAggregator;

use strict;
use warnings;
use Carp ();

use base qw(Exporter);

our @EXPORT = qw(aggregate_joined_columns);

our $VERSION = '0.01';



sub aggregate_joined_columns {
    my ( $itr, $opt ) = @_;

    Carp::croak("Set pk and tags.") unless $opt->{ pk } and $opt->{ tags };

    if ( ref( $itr ) eq 'ARRAY' ) {
        return wantarray ? () : [] unless @$itr;
        $itr = DBIx::JoinedColumnAggregator::DummyIterator->new( $itr );
    }

    my @pks        = ref $opt->{ pk } ? @{ $opt->{ pk } } : $opt->{ pk };
    my $tags       = $opt->{ tags };
    my $setter     = $opt->{ setter } || sub { $_[0]->{ $_[1] } = $_[2] };
    my $nextmethod = $opt->{ next_method } || 'next';
    my $alwayshash = $opt->{ always_hash };
    my $row_object = ($opt->{ access_style } and $opt->{ access_style } eq 'hash') ? 1 : 0;
    my $getter     = ($opt->{ access_style } && ref($opt->{ access_style }) eq 'CODE')
                                                            ? $opt->{ access_style } : undef;

    if ( not $itr->can( $nextmethod ) ) {
        Carp::croak("Iterator must have '$nextmethod' method.");
    }

    # prepare joined columns data
    my ( $cols, $alias );
    my @tags = keys %$tags;
    my @nest;
    for my $ref_name ( @tags ) {
        $cols->{ $ref_name }  = [ map {
             ref($_) eq 'ARRAY' ?  $_->[0]
           : ref($_) eq 'HASH'  ?  _nested_cols( $_, $ref_name, $opt, \@nest )
           : $_;
        } @{ $tags->{ $ref_name } } ];
        $alias->{ $ref_name } = { map {
             ref($_) eq 'ARRAY' ? ($_->[0] => $_->[1])
           : ref($_) eq 'HASH'  ?  _nested_alias( $_ )
           : ($_ => $_)
        } @{ $tags->{ $ref_name } } };
    }

    my %fetched;
    my @representatives;
    my $found_values = {};

    # aggregation
    while ( my $object = $itr->$nextmethod() ) {
        my $pk = $row_object ? join( ":", map { $object->{$_} } @pks ) # set pk data
               : $getter     ? join( ":", map { $getter->( $object, $_ ) } @pks )
               :               join( ":", map { $object->$_() } @pks );

        unless ( $fetched{ $pk } ) {
            $fetched{ $pk } = { object => $object, rows => {} };
            push @representatives, $fetched{ $pk };
        }

        for my $ref_name ( @tags ) { # access joined data
            my $cols  = $cols->{ $ref_name };
            my $alias = $alias->{ $ref_name };
            my @items = $row_object ? map { $alias->{ $_ } => $object->{$_} } @$cols
                      : $getter     ? map { $alias->{ $_ } => $getter->( $object, $_ ) } @$cols
                      :               map { $alias->{ $_ } => $object->$_() } @$cols;
             next if $found_values->{ $pk }->{ $ref_name }
                                    ->{ join( "\0", map { defined $_ ? $_ : "\0NULL\0" } @items ) }++;
             push @{ $fetched{ $pk }->{ rows }->{ $ref_name } },
                                    ( $alwayshash || scalar(@$cols) > 1 ) ? { @items } : $items[1];
        }
    }

    # merging
    for my $obj_and_rows ( @representatives ) {
        my $object = $obj_and_rows->{ object };

        for my $ref_name ( @tags ) {
            my $rows = $obj_and_rows->{ rows }->{ $ref_name };

            if ( @$rows == 1 ) { # Is joined result a null?
                my $row = $rows->[0];
                my $val = ref($row) eq 'HASH'  ?
                                        (scalar(grep { !defined } values %{ $row }) != scalar(keys %{ $row }))
                        : defined($row);
                $rows = [] unless $val;
            }

            $setter->( $object, $ref_name, $rows, $opt );
        }
    }

    my @objects = map { $_->{ object } } @representatives;

    # nest
    for my $nest ( @nest ) {
        my $tag = $nest->[0];
        my $opt = $nest->[1];
        my $setter     = $opt->{ setter } || sub { $_[0]->{ $_[1] } = $_[2] };
        my $row_object = ($opt->{ access_style } and $opt->{ access_style } eq 'hash') ? 1 : 0;
        my $getter     = ($opt->{ access_style } && ref($opt->{ access_style }) eq 'CODE');
        for my $object ( @objects ) {
            my $data = $row_object ? $object->{$tag}
                     : $getter     ? $getter->( $object, $tag )
                     :               $object->$tag();
            my $list_in_nest = aggregate_joined_columns( $data, $opt );
            $setter->( $object, $tag, $list_in_nest, $opt );
        }
    }

    return wantarray ? @objects : \@objects;
}


sub _nested_cols {
    my ( $v, $ref_name, $opt_orig, $nest ) = @_;

    Carp::croak("") unless ( exists $v->{ pk } and exists $v->{ tags } );

    my $opt   = {};
    my $pk    = $v->{ pk };
    my $tags  = $v->{ tags };

    %$opt = (%$opt_orig, %$v);

    push @$nest, [ $ref_name, $opt ];

    return map {
        ref($_) eq 'ARRAY' ? $_->[0]
      : ref($_) eq 'HASH'  ? _nested_cols( $_ )
      : $_
    } map { @{$_} } values %$tags;
}


sub _nested_alias {
    my ( $v ) = @_;
    my ( $pk, @cols ) = ( $v->{pk}, map { @{$_} } values %{$v->{ tags }} );
    ( map {
          ref($_) eq 'ARRAY' ? ($_->[0] => $_->[0])#( $_->[0] => $_->[1] )
        : ref($_) eq 'HASH'  ? ( _nested_alias($_) )
        : ( $_ => $_ )
    } @cols );
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
    
    my @data = aggregate_joined_columns( $sth, {
        pk => ['id'],
        tags => {
            product_codes => ['product_code'],
            groups        => ['group_id', 'group_name'],
        },
        access_style => 'hash',
        next_method  => 'fetchrow_hashref',
    } );
    
    for my $data ( @data ) {
        # $data->{ product_codes } return a list ref of 'product_code'
        # $data->{ groups } return a list ref of hash ref
        #                          having 'group_id' and 'group_name'.
    }
    
    # using ORM like DBIx::Skinny or Teng or etc.
    # use and setup the module...
    # ...
    my $itr = $skinny->search_by_sql( $a_sql_having_lef_join_closures );
    my $data = $aggregate_joined_columns( $itr, {
        pk => ['id'],
        tags => {
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
        # $data->{ extra }->{ groups } return a list ref of hash ref
        #                                   having 'group_id' and 'group_name'.
    }
    

=head1 DESCRIPTION

This module provides a function which aggregates joined columns
in using L<DBI> or L<DBIx::Skinny> or L<Teng> and so on.

You tell C<pk> (primary key) and C<tags> to the function.
C<pk> is used to determine representative objects.
C<tags> is used to determine target columns and a key name
for accessing the aggregated data.

=head2 FLLOW

    * Takes an iterator and iterates to get data objects.
    
    * Makes each first objects with primary key value representative objects.
    
    * Aggregates target columns exception of duplicated value.
    
    * Sets aggregated data into the representative objects.
    
    * Returns a list referance of the representative objects.

NOTE: Duplicated value are ignored. So if you want to aggregate them,
add other unique constrained column (ex. foreign key) into C<tags>
option.

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
        tags => {
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

=item tags

A hash reference of tags for grouping and columns .

    tags => {
        $group_tag1 => [ @tag1_columns ],
        $group_tag2 => [ @tag2_columns ],
    }

Can use array references to set column alias.

    tags => {
        'books' => [ ['book_id' => 'id'], ['books_title' => 'title'], ... ],
        ...
    }
    
    ...
    
    $data->{'books'}->[0]->{'id'}; # get column value instead of 'book_id'

Note: single column with an alias in a tag is ignored.

    tags => {
        'books' => [ ['book_id' => 'id'] ],
        ...
    }
    
    ...
    
    $data->{'books'}->[0]; # get value directly


=item access_style

Getting objects values style. 'hash' or 'method' or a code reference are acceptable.
'method' by default.

You can set a subroutin reference to access data flexibly.

    access_style => sub {
        my ( $object, $column_name ) = @_;
        return $object->get_column( $column_name );
    }

So 'hash' is equivalent to:

    access_style => sub {
        my ( $object, $column_name ) = @_;
        return $object->{ $column_name };
    }

And 'method' equivalent to:

    access_style => sub {
        my ( $object, $column_name ) = @_;
        return $object->$column_name();
    }

=item setter

A subroutine reference that sets aggregated values to objects.
Four arguments are passed into it.
First is an object expected to have aggreaged values.
Second is a group tag set by C<tags> option.
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

=item always_hash

Single target column are aggregated as scalar value.
If you set the option with true value, aggregated as hash reference.

    tags => {
        books => [ 'book_id' ],
    }
    # => $object->{ books } : [ 100, 123, .... ]
    
    
    always_hash => 1,
    tags => {
        books => [ 'book_id' ],
    }
    # => $object->{ books } : [ { book_id => 100 }, { book_id => 123}, .... ]

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

