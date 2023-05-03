MERGE INTO default.target as target
USING source ON target.userId = source.userId
WHEN MATCHED THEN
UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt
WHEN NOT MATCHED
THEN INSERT (userId, info, persona, updatedAt)
VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt);