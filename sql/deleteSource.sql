DELETE FROM
  "sync"."files" f
WHERE
  f."name" = $name;
