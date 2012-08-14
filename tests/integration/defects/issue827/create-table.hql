use '/';
drop table if exists LoadTest;
create table LoadTest (
    Field1,
    INDEX Field1
) COMPRESSOR="none";
