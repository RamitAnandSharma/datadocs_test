DROP DATABASE  IF EXISTS dataparse_test;
DROP ROLE IF EXISTS testuser;


CREATE DATABASE dataparse_test;

CREATE USER testuser;
ALTER USER testuser WITH ENCRYPTED PASSWORD 'testuser';
GRANT ALL PRIVILEGES ON DATABASE dataparse_test TO testuser;


\c dataparse_test;

DROP TABLE IF EXISTS test_table1 ;
DROP TABLE IF EXISTS test_table2 ;

CREATE TABLE test_table1 (
  article_id bigserial primary key,
  article_name varchar(20) NOT NULL,
  article_desc text NOT NULL,
  date_added timestamp default NULL
);

CREATE TABLE test_table2 (
  article_id bigserial primary key,
  article_name varchar(20) NOT NULL,
  article_desc text NOT NULL,
  date_added timestamp default NULL
);
