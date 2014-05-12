use '/';
drop table if exists LoadTest;
CREATE TABLE LoadTest (
  Field,
  ACCESS GROUP default (Field) BLOCKSIZE 1000
) COMPRESSOR="none";
