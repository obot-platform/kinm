INSERT INTO placeholder(id, name, namespace, previous_id, uid, created, deleted, value extra_fields)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM placeholder),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7 extra_vals) RETURNING id;