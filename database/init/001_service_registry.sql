CREATE TABLE IF NOT EXISTS gateway_services (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    route_prefix TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    health_check_path TEXT NOT NULL DEFAULT '/healthz',
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO gateway_services (name, route_prefix, base_url, health_check_path, is_enabled)
VALUES
    ('alpha', '/alpha', 'http://backend-alpha:8000', '/healthz', TRUE),
    ('bravo', '/bravo', 'http://backend-bravo:8000', '/healthz', TRUE),
    ('charlie', '/charlie', 'http://backend-charlie:8000', '/healthz', TRUE)
ON CONFLICT (name)
DO UPDATE SET
    route_prefix = EXCLUDED.route_prefix,
    base_url = EXCLUDED.base_url,
    health_check_path = EXCLUDED.health_check_path,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = NOW();
