drop table if exists LoadTest;
create table LoadTest (
  Field,
  ACCESS GROUP default () bloomfilter='rows+cols --false-positive 0.05'
) COMPRESSOR="none";
