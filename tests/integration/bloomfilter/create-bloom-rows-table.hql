drop table if exists LoadTest;
create table LoadTest (
  Field,
  ACCESS GROUP default () bloomfilter='rows --false-positive 0.01'
) COMPRESSOR="none";
