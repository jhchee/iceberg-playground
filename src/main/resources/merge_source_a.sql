MERGE INTO default.source_a as target
USING source ON target.userId = source.userId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *