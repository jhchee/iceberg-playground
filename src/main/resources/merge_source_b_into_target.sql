MERGE INTO default.target as target USING source ON target.userId = source.userId
WHEN MATCHED
THEN UPDATE SET target.info = struct(source.name), target.updatedAt = source.updatedAt
WHEN NOT MATCHED
THEN INSERT (userId, info, persona, updatedAt)
VALUES (source.userId, struct(source.name), NULL, source.updatedAt)