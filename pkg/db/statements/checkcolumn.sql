SELECT 1 FROM information_schema.columns
WHERE table_name = 'placeholder'
AND table_schema = COALESCE(NULLIF(current_schema(), ''), 'public')
AND (column_name = 'new_column' OR column_name = 'new_column_lower');
