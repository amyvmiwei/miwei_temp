use '/';
select * from OffsetTest offset 0 limit 3 keys_only;
select * from OffsetTest offset 10 limit 3 keys_only;
select * from OffsetTest offset 100 limit 3 keys_only;

# the next query passes the boundary between the 1st and the 2nd range
select * from OffsetTest offset 990 limit 30 keys_only;

select * from OffsetTest offset 890 limit 300 keys_only;

select * from OffsetTest offset 5000 limit 3 keys_only;
select * from OffsetTest offset 15000 limit 3 keys_only;
select * from OffsetTest offset 49995 limit 5 keys_only;
select * from OffsetTest offset 49998 limit 5 keys_only;
select * from OffsetTest offset 45000 keys_only;
