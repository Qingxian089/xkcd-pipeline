-- Initialize schema and table for raw xkcd comics data
CREATE SCHEMA IF NOT EXISTS xkcd;

CREATE TABLE IF NOT EXISTS xkcd.raw_xkcd_comics (
    num INTEGER PRIMARY KEY,          -- Unique comic ID
    title TEXT,                       -- Comic title
    alt_text TEXT,                     -- Alternative text description
    img_url TEXT,                      -- Direct image URL
    published_date DATE,               -- Date of publication (YYYY-MM-DD)
    transcript TEXT,                   -- Comic transcript text
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,  -- Data collection timestamp
    raw_data JSONB                     -- Raw JSON response from API
);