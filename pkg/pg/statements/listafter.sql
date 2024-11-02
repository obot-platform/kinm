SELECT (SELECT max(id) FROM placeholder)           AS max_id,
       coalesce((SELECT c.id
                 FROM compaction AS c
                 WHERE c.name = 'placeholder'), 0) as compaction_id,
       id,
       name,
       namespace,
       previous_id,
       uid,
       created OR previous_id IS NULL AS created,
       deleted,
       value
FROM placeholder
WHERE (namespace = $1 OR $1 IS NULL)
  AND (name = $2 OR $2 IS NULL)
  AND id > $3
ORDER BY id