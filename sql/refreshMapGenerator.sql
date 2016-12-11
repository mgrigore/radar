CREATE OR REPLACE FUNCTION
  "sync"."map"("snapshot" timestamp with time zone, "period" interval, "idle" integer, "overload" integer)
RETURNS
  SETOF "sync"."record"
AS $$
  DECLARE
    source RECORD;

  BEGIN
    CREATE TEMPORARY TABLE
      "projection"
    OF
      "sync"."record"
    ON COMMIT DROP;

    FOR
      source
    IN SELECT
      "data"
    FROM
      "sync"."files" f
    WHERE
      f."modified" BETWEEN (snapshot - period) AND (snapshot)
    LOOP
      EXECUTE '
        INSERT INTO
          "projection"("circumscriptie", "sectie", "voturi")
        SELECT
          j."circumscriptie",
          s."Nr sectie de votare" AS "sectie",
          s."LT" AS "voturi"
        FROM
          ' || source."data"::regclass || ' s
        JOIN
          "live"."judete" j
        ON
          s."Judet" = j."abreviere"
      ';
    END LOOP;
    
    RETURN QUERY
      WITH
        overview
      AS (
        SELECT
          "circumscriptie",
          "sectie",
          MAX("voturi") AS "voturi",
          (MAX("voturi") - MIN("voturi")) AS "delta"
        FROM
          "projection" p
        GROUP BY
          1,
          2
      )
      SELECT
        o."circumscriptie",
        o."sectie",
        o."voturi",
        (CASE
          WHEN o."delta" < idle     THEN 1
          WHEN o."delta" > overload THEN 3
          ELSE                           2
        END)::smallint AS "ocupare"
      FROM
        overview o
      ORDER BY
        1,
        2;

    RETURN;
  END;
$$ LANGUAGE plpgsql VOLATILE RETURNS NULL ON NULL INPUT;
