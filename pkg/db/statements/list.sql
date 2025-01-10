SELECT (SELECT max(id) FROM placeholder) AS max_id,
       coalesce((SELECT c.id
                 FROM compaction AS c
                 WHERE c.name = 'placeholder'), 0) as compaction_id,
       id,
       name,
       namespace,
       previous_id,
       uid,
       CASE WHEN created = 1 OR previous_id IS NULL THEN 1 ELSE 0 END AS created,
       deleted,
       value
FROM (SELECT id,
             name,
             namespace,
             previous_id,
             uid,
             created,
             deleted,
             value, field_names
             row_number() OVER (PARTITION BY name, namespace
                 ORDER BY ID DESC) AS rn
      FROM placeholder
      WHERE (namespace = $1 OR $1 IS NULL)
        AND (name = $2 OR $2 IS NULL)
        AND ($3 = 0 OR id <= $3)
        AND ($4 = 0 OR id > $4)) AS r
WHERE rn = 1
  AND deleted = 0 extra_fields
ORDER BY id