-- this migration exists solely for the purpose of running the included tests
-- set run time parameters
alter database audit_star SET audit_star.changed_by = 'default';

-- set enough schema for audit testing
create extension if not exists hstore;
drop schema if exists teststar cascade;
drop schema if exists teststar_audit cascade;
drop schema if exists teststar_audit_raw cascade;
drop schema if exists schema_skipme cascade;
drop schema if exists schema_skipme_audit cascade;
drop schema if exists schema_skipme_audit_raw cascade;
drop schema if exists accounting cascade;
drop schema if exists accounting_audit cascade;
drop schema if exists accounting_audit_raw cascade;
drop schema if exists pgs_audit cascade;
drop schema if exists pgs_audit_raw cascade;
drop schema if exists teststar_2 cascade;
drop schema if exists teststar_2_audit cascade;
drop schema if exists teststar_2_audit_raw cascade;
drop schema if exists teststar_3 cascade;
drop schema if exists teststar_3_audit cascade;
drop schema if exists teststar_3_audit_raw cascade;
drop schema if exists teststar_quote cascade;
drop schema if exists teststar_quote_audit cascade;
drop schema if exists teststar_quote_audit_raw cascade;
drop schema if exists "test:star" cascade;
drop schema if exists "test:star_audit" cascade;
drop schema if exists "test:star_audit_raw" cascade;
drop role if exists test__owner;
drop role if exists not_test__owner;
drop role if exists definitely_not_test__owner;
drop role if exists "7357:owner";
create role test__owner login nosuperuser inherit nocreatedb nocreaterole noreplication;
create role not_test__owner login nosuperuser inherit nocreatedb nocreaterole noreplication;
create role definitely_not_test__owner login nosuperuser inherit nocreatedb nocreaterole noreplication;
create role "7357:owner" login nosuperuser inherit nocreatedb nocreaterole noreplication;
CREATE SCHEMA teststar authorization test__owner;
    --Single column primary key
    create table teststar.table1 (
        id int,
        column2 text,
        column3 numeric(8,2),
        updated_by text,
        constraint testtable1_pk PRIMARY KEY (id)
    );
    alter table teststar.table1 owner to test__owner;
    --Compound primary key
    create table teststar.table2 (
        id int,
        id2 int,
        column3 text,
        updated_by text,
        constraint testtable2_pk PRIMARY KEY(id, id2)
    );
    alter table teststar.table2 owner to test__owner;
    --Table missing updated_by
    create table teststar.table3 (
        id int,
        column2 text,
        constraint testtable3_pk PRIMARY KEY(id)
    );
    alter table teststar.table3 owner to test__owner;
    --Table in exclusion list
    create table teststar.table_skipme (
        id int,
        column2 text,
        constraint tableskipme_pk PRIMARY KEY(id)
    );
    alter table teststar.table_skipme owner to test__owner;
--Schema in exclusion list
create schema schema_skipme authorization test__owner;
    --Table in skipped schema
    create table schema_skipme.table_skipme2 (
        id int,
        column2 text,
        constraint tableskipme2_pk PRIMARY KEY(id)
    );
    alter table schema_skipme.table_skipme2 owner to test__owner;
  create schema teststar_quote authorization test__owner;
  create extension if not exists hstore;
      --quoted column name
      create table teststar_quote.table5 (
        id int,
        ":this_column" text,
        updated_by text,
        constraint testtable5_pk PRIMARY KEY (id)
      );
      alter table teststar_quote.table5 owner to test__owner;
      --quoted table name
      create table teststar_quote."table_:six" (
        id int,
        column2 text,
        updated_by text,
        constraint testtable6_pk PRIMARY KEY (id)
      );
      alter table teststar_quote."table_:six" owner to test__owner;
      --quoted PK
      create table teststar_quote.table7 (
        "i:d" int,
        column2 text,
        updated_by text,
        constraint testtable7_pk PRIMARY KEY ("i:d")
      );
      alter table teststar_quote.table7 owner to test__owner;
      --quoted owner
      create table teststar_quote.table8 (
        id int,
        column2 text,
        updated_by text,
        constraint testtable8_pk PRIMARY KEY (id)
      );
      alter table teststar_quote.table8 owner to "7357:owner";
  create schema "test:star" authorization test__owner;
  create extension if not exists hstore;
      --quoted schema name
      create table "test:star".table1 (
        id int,
        column2 text,
        updated_by text,
        constraint testtable1_pk PRIMARY KEY (id)
      );
      alter table "test:star".table1 owner to test__owner;
  --Schema owned by other owner
  create schema teststar_2 authorization not_test__owner;
    create table teststar_2.table1 (
      id SERIAL PRIMARY KEY,
      column2 text
    );
    alter table teststar_2.table1 owner to not_test__owner;
    create table teststar_2.table2 (
      id SERIAL PRIMARY KEY,
      column2 text
    );
    alter table teststar_2.table2 owner to test__owner;
  --Schema owned by other owner
  create schema teststar_3 authorization definitely_not_test__owner;
    create table teststar_3.table1 (
      id SERIAL PRIMARY KEY,
      column2 text
    );
    alter table teststar_3.table1 owner to definitely_not_test__owner;
    --Table owned by other role
  create table teststar_2.table4 (
    id int,
    column2 text,
    updated_by text,
    constraint testtable4_pk PRIMARY KEY (id)
  );
  alter table teststar_2.table4 owner to not_test__owner;
  create schema pgs authorization test__owner;
  create table pgs.teststar_1(id serial);
  alter table pgs.teststar_1 owner to test__owner;
