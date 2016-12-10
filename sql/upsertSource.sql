INSERT INTO
  "sync"."files"("name", "modified", "data")
VALUES
  ($name, $modified, $data)
ON CONFLICT ("name") DO UPDATE
SET
  "modified" = EXCLUDED."modified",
  "data"     = EXCLUDED."data";
