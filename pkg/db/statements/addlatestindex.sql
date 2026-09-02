CREATE INDEX IF NOT EXISTS idx_placeholder_latest ON placeholder (name, namespace, id DESC);
