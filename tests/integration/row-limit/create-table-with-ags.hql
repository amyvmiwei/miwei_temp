drop table if exists LoadTest;
create table LoadTest (
    Field1,
    Field2,
    Field3,
    ACCESS GROUP a (Field1),
    ACCESS GROUP b (Field2),
    ACCESS GROUP c (Field3)
) COMPRESSOR="none" BLOCKSIZE=5000;

