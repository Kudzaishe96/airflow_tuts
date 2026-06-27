-- init-scripts/init.sql

-- 1. Create your custom tracking database
CREATE DATABASE chess_analytics;

-- 2. Connect to the new database to configure it
\c chess_analytics;

-- 3. Pre-create your chess schema so Spark can write to it immediately
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

-- Explicitly target your bronze schema inside the chess_analytics database
CREATE TABLE IF NOT EXISTS bronze.kue_daily (
    daily_id                      SERIAL PRIMARY KEY,    -- Incremental primary key
    daily_last_rating             INT,                   -- Chess ratings are whole numbers
    daily_last_date               VARCHAR(255),             -- Converted to human-readable timestamp
    daily_last_rd                 INT,                   -- Rating Deviation
    daily_best_rating             INT,                   -- Peak rating
    daily_best_date               VARCHAR(255),             -- Peak rating timestamp
    daily_best_game               TEXT,                  -- URL of your best game
    daily_record_win              INT,                   -- Total wins
    daily_record_loss             INT,                   -- Total losses
    daily_record_draw             INT,                   -- Total draws
    daily_record_time_per_move    INT,                   -- Seconds per move
    daily_record_timeout_per_move INT,                   -- Timeout count
    daily_created_date            DATE DEFAULT CURRENT_DATE, -- System date of record insertion
    daily_created_time            TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- System clock time of record insertion
);

CREATE TABLE IF NOT EXISTS bronze.kue_rapid (
    rapid_id SERIAL PRIMARY KEY,                         -- Incremental primary key
    rapid_last_rating INT,                               -- Chess ratings are whole numbers
    rapid_last_date VARCHAR(255),                           -- Converted to human-readable timestamp
    rapid_last_rd INT,                                   -- Rating Deviation
    rapid_best_rating INT,                               -- Peak rating
    rapid_best_date VARCHAR(255),                           -- Peak rating timestamp
    rapid_best_game TEXT,                                -- URL of your best game
    rapid_record_win INT,                                -- Total wins
    rapid_record_loss INT,                               -- Total losses
    rapid_record_draw INT,                               -- Total draws
    rapid_created_date DATE DEFAULT CURRENT_DATE,       -- Creation date
    rapid_created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP         -- Creation time    
);

CREATE TABLE IF NOT EXISTS bronze.kue_blitz (
    blitz_id SERIAL PRIMARY KEY,                         -- Incremental primary key
    blitz_last_rating INT,                               -- Chess ratings are whole numbers
    blitz_last_date VARCHAR(255),                           -- Converted to human-readable timestamp
    blitz_last_rd INT,                                   -- Rating Deviation
    blitz_best_rating INT,                               -- Peak rating
    blitz_best_date VARCHAR(255),                           -- Peak rating timestamp
    blitz_best_game TEXT,                                -- URL of your best game
    blitz_record_win INT,                                -- Total wins
    blitz_record_loss INT,                               -- Total losses
    blitz_record_draw INT,                               -- Total draws
    blitz_created_date DATE DEFAULT CURRENT_DATE,       -- Creation date
    blitz_created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- Creation time    
);

-- Target your bronze layer for raw data ingestion
CREATE TABLE IF NOT EXISTS bronze.daily_leaderboards (
    player_id BIGINT PRIMARY KEY,              -- Using BIGINT as IDs can surpass standard integer limits
    api_id_url TEXT NOT NULL,                 -- Map '@id' column (reserved word in some contexts, renamed for clarity)
    profile_url TEXT,                         -- Player's Chess.com profile link
    username VARCHAR(100) NOT NULL,           -- Unique player handle
    score INT NOT NULL,                       -- Current leaderboard rating points
    rank INT NOT NULL,                        -- Current standing position (1, 2, 3...)
    country_api_url TEXT,                     -- Country metadata API endpoint
    country_id INT,                           -- Country identifier code
    title VARCHAR(10),                        -- Chess title (GM, IM, FM, or NULL for nan)
    player_name VARCHAR(255),                 -- Real name (Can be NULL for nan)
    account_status VARCHAR(50),               -- premium / basic
    avatar_url TEXT,                          -- Image link to the player's avatar
    flair_code VARCHAR(50),                   -- Decorative badge/flair string
    win_count INT DEFAULT 0,                  -- Performance stats tracking
    loss_count INT DEFAULT 0,
    draw_count INT DEFAULT 0
    created_date DATE DEFAULT CURRENT_DATE,       -- Creation date
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP      -- Creation time    
);

-- Index the score and rank for faster leaderboards queries later
CREATE INDEX IF NOT EXISTS idx_daily_leaderboards_score_rank 
ON bronze.daily_leaderboards (score DESC, rank ASC);

-- Target your bronze layer for raw data ingestion
CREATE TABLE IF NOT EXISTS bronze.live_rapid_leaderboards (
    player_id BIGINT PRIMARY KEY,              -- Using BIGINT as IDs can surpass standard integer limits
    api_id_url TEXT NOT NULL,                 -- Map '@id' column (reserved word in some contexts, renamed for clarity)
    profile_url TEXT,                         -- Player's Chess.com profile link
    username VARCHAR(100) NOT NULL,           -- Unique player handle
    score INT NOT NULL,                       -- Current leaderboard rating points
    rank INT NOT NULL,                        -- Current standing position (1, 2, 3...)
    country_api_url TEXT,                     -- Country metadata API endpoint
    country_id INT,                           -- Country identifier code
    title VARCHAR(10),                        -- Chess title (GM, IM, FM, or NULL for nan)
    player_name VARCHAR(255),                 -- Real name (Can be NULL for nan)
    account_status VARCHAR(50),               -- premium / basic
    avatar_url TEXT,                          -- Image link to the player's avatar
    flair_code VARCHAR(50),                   -- Decorative badge/flair string
    win_count INT DEFAULT 0,                  -- Performance stats tracking
    loss_count INT DEFAULT 0,
    draw_count INT DEFAULT 0
    leaderboards_created_date DATE DEFAULT CURRENT_DATE,       -- Creation date
    leaderboards_created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP       -- Creation time    
);

-- Index the score and rank for faster leaderboards queries later
CREATE INDEX IF NOT EXISTS idx_live_rapid_leaderboards_score_rank 
ON bronze.live_rapid_leaderboards (score DESC, rank ASC);


-- Target your bronze layer for raw data ingestion
CREATE TABLE IF NOT EXISTS bronze.live_blitz_leaderboards (
    player_id BIGINT PRIMARY KEY,              -- Using BIGINT as IDs can surpass standard integer limits
    api_id_url TEXT NOT NULL,                 -- Map '@id' column (reserved word in some contexts, renamed for clarity)
    profile_url TEXT,                         -- Player's Chess.com profile link
    username VARCHAR(100) NOT NULL,           -- Unique player handle
    score INT NOT NULL,                       -- Current leaderboard rating points
    rank INT NOT NULL,                        -- Current standing position (1, 2, 3...)
    country_api_url TEXT,                     -- Country metadata API endpoint
    country_id INT,                           -- Country identifier code
    title VARCHAR(10),                        -- Chess title (GM, IM, FM, or NULL for nan)
    player_name VARCHAR(255),                 -- Real name (Can be NULL for nan)
    account_status VARCHAR(50),               -- premium / basic
    avatar_url TEXT,                          -- Image link to the player's avatar
    flair_code VARCHAR(50),                   -- Decorative badge/flair string
    win_count INT DEFAULT 0,                  -- Performance stats tracking
    loss_count INT DEFAULT 0,
    draw_count INT DEFAULT 0
    leaderboards_created_date DATE DEFAULT CURRENT_DATE,       -- Creation date
    leaderboards_created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP       -- Creation time    
);

-- Index the score and rank for faster leaderboards queries later
CREATE INDEX IF NOT EXISTS idx_live_blitz_leaderboards_score_rank 
ON bronze.live_blitz_leaderboards (score DESC, rank ASC);