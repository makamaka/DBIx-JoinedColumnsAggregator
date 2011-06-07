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

my $itr = DummyTestIterator->new( \@objects );

$list = aggregate_joined_columns( $itr, {
    pk => ['id'],
    refs => {
        books   => ['book_id', 'title'],
        groups  => ['group_id'],
    },
    access_style => sub {
        my ( $obj, $name ) = @_;
        $obj->{ $name };
    },
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



$itr = DummyTestIterator2->new( \@objects );

$list = aggregate_joined_columns( $itr, {
    pk => ['id'],
    refs => {
        books   => ['book_id', 'title'],
        groups  => ['group_id'],
    },
    access_style => 'method',
} );

is( scalar(@$list), 4 );

$user = $list->[0];
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



package DummyTestIterator;

sub new {
    my ( $class, $list_ref ) = @_;
    bless { data => $list_ref, idx => 0 }, $class;
}


sub next {
    $_[0]->{ data }->[ $_[0]->{ idx }++ ];
}

package DummyTestIterator2;

sub new {
    my ( $class, $list_ref ) = @_;
    bless { data => $list_ref, idx => 0 }, $class;
}


sub next {
    my $obj = $_[0]->{ data }->[ $_[0]->{ idx }++ ];
    return unless $obj;
    bless $obj, 'DummyTestIterator2::Object';
}


package DummyTestIterator2::Object;

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;
    my $name =  $AUTOLOAD;
    $name =~ s/.*:://;
    return if $name =~ /^[A-Z]+$/;
    $self->{ $name };
}

