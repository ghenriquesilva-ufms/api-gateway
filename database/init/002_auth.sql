CREATE TABLE IF NOT EXISTS gateway_users (
    id BIGSERIAL PRIMARY KEY,
    subject TEXT NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gateway_api_keys (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES gateway_users(id) ON DELETE CASCADE,
    key_name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, key_name)
);

INSERT INTO gateway_users (subject, is_active)
VALUES ('machine-client', TRUE)
ON CONFLICT (subject)
DO UPDATE SET
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO gateway_api_keys (user_id, key_name, key_hash, is_active, expires_at)
SELECT u.id, 'dev-machine-key', '06799f301caa337748af2642bfb72b74b5c0f20de2c688c9c17a2410046bcff3', TRUE, NULL
FROM gateway_users AS u
WHERE u.subject = 'machine-client'
ON CONFLICT (key_hash)
DO UPDATE SET
    is_active = EXCLUDED.is_active,
    expires_at = EXCLUDED.expires_at,
    updated_at = NOW();
