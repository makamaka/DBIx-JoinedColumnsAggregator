
use strict;
use warnings;
use DBI;

sub init {

my $dbh = DBI->connect( 'dbi:SQLite:', '', '' );

$dbh->do(q{ CREATE TABLE users (
    id      integer,
    name    text,
    PRIMARY KEY ( id )
) });

$dbh->do(q{ CREATE TABLE books (
    id          integer,
    title       text,
    PRIMARY KEY ( id )
) });

$dbh->do(q{ CREATE TABLE groups (
    id          integer,
    name        text,
    PRIMARY KEY ( id )
) });

$dbh->do(q{ CREATE TABLE user_book_rels (
    user_id       integer,
    book_id       integer,
    PRIMARY KEY ( user_id, book_id )
) });

$dbh->do(q{ CREATE TABLE user_group_rels (
    group_id      integer,
    user_id       integer,
    PRIMARY KEY ( group_id, user_id )
) });


$dbh->do(q{ INSERT INTO users (id, name) VALUES ( 1, 'AAA' ) }); # has no books, no groups
$dbh->do(q{ INSERT INTO users (id, name) VALUES ( 2, 'BBB' ) }); # has one book, no groups
$dbh->do(q{ INSERT INTO users (id, name) VALUES ( 3, 'CCC' ) }); # has two books, one group
$dbh->do(q{ INSERT INTO users (id, name) VALUES ( 4, 'DDD' ) }); # has three books, two groups

$dbh->do(q{ INSERT INTO books (id, title) VALUES ( 1, 'book A' ) });
$dbh->do(q{ INSERT INTO books (id, title) VALUES ( 2, 'book B' ) });
$dbh->do(q{ INSERT INTO books (id, title) VALUES ( 3, 'book C' ) });

$dbh->do(q{ INSERT INTO groups (id, name) VALUES ( 1, 'group A' ) });
$dbh->do(q{ INSERT INTO groups (id, name) VALUES ( 2, 'group B' ) });

$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 2, 1 ) });
$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 3, 1 ) });
$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 3, 2 ) });
$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 4, 1 ) });
$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 4, 2 ) });
$dbh->do(q{ INSERT INTO user_book_rels (user_id, book_id) VALUES ( 4, 3 ) });

$dbh->do(q{ INSERT INTO user_group_rels (user_id, group_id) VALUES ( 3, 1 ) });
$dbh->do(q{ INSERT INTO user_group_rels (user_id, group_id) VALUES ( 4, 1 ) });
$dbh->do(q{ INSERT INTO user_group_rels (user_id, group_id) VALUES ( 4, 2 ) });

return $dbh;
}


sub join_sql {

q{
    SELECT u.id, u.name, b.id AS book_id, b.title, g.id AS group_id, g.name AS group_name  FROM users u
        LEFT JOIN user_book_rels book_rel ON u.id = book_rel.user_id
            LEFT JOIN books b ON b.id = book_rel.book_id
        LEFT JOIN user_group_rels group_rel ON u.id = group_rel.user_id
            LEFT JOIN groups g ON g.id = group_rel.group_id
}

}


1;
