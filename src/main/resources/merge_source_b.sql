MERGE INTO default.source_b as target
USING source ON target.userId = source.userId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *