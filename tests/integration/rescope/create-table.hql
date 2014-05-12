use '/';
drop table if exists LoadTest;
CREATE TABLE LoadTest (
  Field1,
  Field2,
  ACCESS GROUP INMEMORY (Field1) bloomfilter="none" IN_MEMORY
);
