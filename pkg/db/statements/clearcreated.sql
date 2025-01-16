UPDATE placeholder
SET created = NULL, deleted = 1
WHERE namespace = $1
  AND name = $2
  AND id < $3
  AND created = 1;