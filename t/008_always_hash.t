use lib './t';
use Test::More;
use strict;
use warnings;
use DBIx::JoinedColumnsAggregator;
use Data::Dumper;

BEGIN {
    eval "use DBD::SQLite";
    plan skip_all => "DBD::SQLite is not installed. skip testing" if $@;
}

require "t/lib/setup.pl";

my $dbh = init();
my $sth;

ok( $dbh );

is( $dbh->selectcol_arrayref(q{ SELECT count(*) FROM users  })->[0], 4, 'users table');
is( $dbh->selectcol_arrayref(q{ SELECT count(*) FROM books  })->[0], 3, 'books table');
is( $dbh->selectcol_arrayref(q{ SELECT count(*) FROM groups })->[0], 2, 'groups table');
is( $dbh->selectcol_arrayref(q{ SELECT count(*) FROM user_book_rels  })->[0], 6, 'user-book table');
is( $dbh->selectcol_arrayref(q{ SELECT count(*) FROM user_group_rels })->[0], 3, 'user-group table');

my ( $sql, $list );

$sql = join_sql();

$sth = $dbh->prepare(qq{ $sql WHERE u.id = 4 ORDER BY b.id ASC, g.id ASC } );

ok( $sth->execute );

eval {
    aggregate_joined_columns( $sth, {
        pk => ['id'],
        tags => {
            books   => ['book_id', 'title'],
            groups  => ['group_id'],
        },
        access_style => 'hash',
    } );
};
like( $@, qr/Iterator must have/ );

#
#
#

$list = aggregate_joined_columns( $sth, {
    pk => ['id'],
    tags => {
        books   => ['book_id', 'title'],
        groups  => ['group_id'],
    },
    access_style => 'hash',
    next_method  => 'fetchrow_hashref',
    always_hash  => 1,
} );

is( scalar(@$list), 1 );

my $user = $list->[0];

is( $user->{id}, 4 );
is( $user->{name}, 'DDD' );

is_deeply( $user->{ books }, [
    { book_id => 1, title => 'book A' },
    { book_id => 2, title => 'book B' },
    { book_id => 3, title => 'book C' },
] );

is_deeply( $user->{ groups }, [
    { group_id => 1 },
    { group_id => 2 }
] );

#
#
#

$sth = $dbh->prepare(qq{ $sql WHERE u.id = 4 ORDER BY b.id ASC, g.id ASC } );

$sth->execute;

$list = aggregate_joined_columns( $sth, {
    pk => ['id'],
    tags => {
        books   => ['book_id'],
        groups  => ['group_id', 'group_name'],
    },
    access_style => 'hash',
    next_method  => 'fetchrow_hashref',
    always_hash  => 1,
    setter => sub {
        my ( $obj, $ref_name, $rows, $opt ) = @_;
        $obj->{ "_$ref_name" } = $rows;
    },
} );


is( scalar(@$list), 1 );

$user = $list->[0];

is( $user->{id}, 4 );
is( $user->{name}, 'DDD' );

is_deeply( $user->{ _books }, [
    { book_id => 1 },
    { book_id => 2 },
    { book_id => 3 },
] );

is_deeply( $user->{ _groups }, [
    { group_id => 1, group_name => 'group A' },
    { group_id => 2, group_name => 'group B' },
] );


#
#
#

$sth = $dbh->prepare(qq{ $sql WHERE u.id = 4 ORDER BY b.id ASC, g.id ASC } );

$sth->execute;

$list = aggregate_joined_columns( $sth, {
    pk => ['id'],
    tags => {},
    access_style => 'hash',
    next_method  => 'fetchrow_hashref',
} );

is( scalar(@$list), 1 );

$user = $list->[0];

is( $user->{id}, 4 );
is( $user->{name}, 'DDD' );



done_testing;

