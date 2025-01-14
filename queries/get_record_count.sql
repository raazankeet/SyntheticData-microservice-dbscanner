SELECT SUM(row_count) 
FROM sys.dm_db_partition_stats 
WHERE object_id = OBJECT_ID(?) AND index_id <= 1;
