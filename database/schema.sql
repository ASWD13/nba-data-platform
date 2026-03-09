CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER,
    player_name TEXT,
    team TEXT,
    points FLOAT,
    assists FLOAT,
    rebounds FLOAT,
    steals FLOAT,
    blocks FLOAT,
    minutes FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);