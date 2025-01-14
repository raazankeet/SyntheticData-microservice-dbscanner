WITH PrimaryKeyColumns AS (
    SELECT 
        kcu.COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE tc.TABLE_NAME = ? 
      AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
),
TableColumns AS (
    SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.IS_NULLABLE,
        col.is_identity
    FROM INFORMATION_SCHEMA.COLUMNS c
    JOIN sys.columns col
        ON col.name = c.COLUMN_NAME
        AND col.object_id = OBJECT_ID(?)
    WHERE c.TABLE_NAME = ?
)
SELECT 
    tc.COLUMN_NAME, 
    tc.DATA_TYPE, 
    tc.CHARACTER_MAXIMUM_LENGTH, 
    CASE 
        WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 
        ELSE 0 
    END AS PRIMARY_KEY,
    CASE 
        WHEN tc.IS_NULLABLE = 'YES' THEN 1 
        ELSE 0 
    END AS NULLABLE,
    CASE 
        WHEN tc.is_identity = 1 THEN 1 
        ELSE 0 
    END AS IS_IDENTITY
FROM TableColumns tc
LEFT JOIN PrimaryKeyColumns pk
    ON tc.COLUMN_NAME = pk.COLUMN_NAME;
