use '/';
drop table if exists LoadTest;
create table LoadTest (
    Field1,
    Field2,
    Field3
) COMPRESSOR="none";
