create table author (
  id number,
  name varchar2(100),
  primary key(id)
);

create sequence author_sequence start with 1;

create table book (
  id number,
  title varchar2(100),
  genre varchar2(20),
  author_id number,
  primary key(id),
  foreign key (author_id) references author(id)
);

create sequence book_sequence start with 1;