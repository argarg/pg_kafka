/* contrib/seg/seg--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION kafka UPDATE TO '0.2'" to load this file. \quit

create function kafka.flush() 
returns boolean as 'pg_kafka.so', 'pg_kafka_flush'
language C immutable;

comment on function kafka.flush() is 'Flushes messages.';
