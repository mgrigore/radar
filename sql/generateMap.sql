SELECT
  *
FROM
  "sync"."map"(
    $latest,
    ($minutes::text || ' minutes')::interval,
    $idle,
    $overload
  );
