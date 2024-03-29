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

$sth = $dbh->prepare(qq{ $sql ORDER BY u.id ASC, b.id ASC, g.id ASC } );

ok( $sth->execute );

#
#
#

my @objects;
while ( my $hash = $sth->fetchrow_hashref ) {
    push @objects, $hash;
}


$list = aggregate_joined_columns( \@objects, {
    pk => ['id'],
    tags => {
        books   => ['book_id', 'title'],
        groups  => ['group_id'],
    },
    access_style => 'hash',
} );

is( scalar(@$list), 4 );

my $user = $list->[0];
is( $user->{id}, 1 );
is( $user->{name}, 'AAA' );
is_deeply( $user->{ books }, [] );
is_deeply( $user->{ groups }, [] );

$user = $list->[1];
is( $user->{id}, 2 );
is( $user->{name}, 'BBB' );
is_deeply( $user->{ books }, [
    { book_id => 1, title => 'book A' },
] );
is_deeply( $user->{ groups }, [] );

$user = $list->[2];
is( $user->{id}, 3 );
is( $user->{name}, 'CCC' );
is_deeply( $user->{ books }, [
    { book_id => 1, title => 'book A' },
    { book_id => 2, title => 'book B' },
] );
is_deeply( $user->{ groups }, [1] );

$user = $list->[3];
is( $user->{id}, 4 );
is( $user->{name}, 'DDD' );
is_deeply( $user->{ books }, [
    { book_id => 1, title => 'book A' },
    { book_id => 2, title => 'book B' },
    { book_id => 3, title => 'book C' },
] );
is_deeply( $user->{ groups }, [1,2] );


done_testing;

