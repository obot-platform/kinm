DELETE FROM placeholder WHERE id in (
    SELECT id
    FROM (SELECT id,
                 deleted,
                 created,
                 previous_id,
                 row_number() OVER (PARTITION BY name, namespace ORDER BY ID DESC) AS rn
          FROM placeholder
          WHERE id <= coalesce(
                  (SELECT id
                   FROM compaction
                   WHERE name = 'placeholder')
              , 0)
          )
    WHERE deleted = 1 OR (rn > 1 AND created IS NULL) OR (previous_id IS NULL AND created IS NULL)
    ORDER BY id
    LIMIT 500
    );
