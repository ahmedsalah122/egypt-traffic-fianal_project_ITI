-- =============================================================================
-- Egypt Traffic — Gold Layer DDL
-- PostgreSQL · Star Schema
-- Tables: dim_date, dim_time, dim_location, dim_incident_category,
--         fact_traffic_flow, fact_incidents
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS postgis;   -- for geo queries on lat/lon
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;


-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_date
-- Populated once via generation script for the full project date range.
-- Includes Egyptian calendar enrichment.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT             PRIMARY KEY,   -- YYYYMMDD integer e.g. 20240101
    full_date           DATE            NOT NULL UNIQUE,
    year                SMALLINT        NOT NULL,
    quarter             SMALLINT        NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month               SMALLINT        NOT NULL CHECK (month   BETWEEN 1 AND 12),
    month_name_en       VARCHAR(10)     NOT NULL,
    month_name_ar       VARCHAR(20)     NOT NULL,
    week_of_year        SMALLINT        NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    day_of_month        SMALLINT        NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week         SMALLINT        NOT NULL CHECK (day_of_week  BETWEEN 1 AND 7),  -- 1=Mon
    day_name_en         VARCHAR(10)     NOT NULL,
    day_name_ar         VARCHAR(20)     NOT NULL,
    is_weekend          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_public_holiday   BOOLEAN         NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    is_ramadan          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_school_term      BOOLEAN         NOT NULL DEFAULT FALSE,
    season              VARCHAR(10)     NOT NULL,   -- Spring / Summer / Autumn / Winter
    day_type            VARCHAR(20)     NOT NULL    -- Weekday / Weekend / Holiday / Ramadan
);

COMMENT ON TABLE dim_date IS
    'Date dimension — one row per calendar day, covers full project range. '
    'Populated once via generation script; updated manually for new holidays.';


-- ---------------------------------------------------------------------------
-- dim_time
-- 96 rows — one per 15-minute slot across 24 hours (00:00 … 23:45).
-- Populated once; never changes.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_time (
    time_key        SMALLINT    PRIMARY KEY,   -- 0..95  (slot index)
    hour            SMALLINT    NOT NULL CHECK (hour   BETWEEN 0 AND 23),
    minute          SMALLINT    NOT NULL CHECK (minute IN (0, 15, 30, 45)),
    period_label    VARCHAR(10) NOT NULL,      -- e.g. '07:30'
    is_peak_am      BOOLEAN     NOT NULL DEFAULT FALSE,   -- 07:00–09:45
    is_peak_pm      BOOLEAN     NOT NULL DEFAULT FALSE,   -- 16:00–19:45
    is_off_peak     BOOLEAN     NOT NULL DEFAULT FALSE,
    is_overnight    BOOLEAN     NOT NULL DEFAULT FALSE,   -- 00:00–04:45
    UNIQUE (hour, minute)
);

COMMENT ON TABLE dim_time IS
    '96-row time dimension for 15-minute granularity. '
    'Seeded once; time_key = hour*4 + minute/15.';


-- ---------------------------------------------------------------------------
-- dim_location
-- SCD Type 2 — each reclassification creates a new version row.
-- Natural key: location_key (location_name string from the flow source).
-- Surrogate key: location_sk (SERIAL).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_location (
    location_sk     SERIAL          PRIMARY KEY,
    location_key    VARCHAR(100)    NOT NULL,   -- natural key e.g. 'tahrir_square'
    version_number  SMALLINT        NOT NULL DEFAULT 1,
    location_name   VARCHAR(100)    NOT NULL,   -- human-readable label
    city            VARCHAR(50)     NOT NULL,
    governorate     VARCHAR(50),
    area_zone       VARCHAR(100),
    road_type       VARCHAR(50),                -- arterial / highway / local …
    frc             VARCHAR(5),                 -- FRC0..FRC6 from flow source
    frc_label       VARCHAR(50),
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    geom            GEOMETRY(Point, 4326),      -- PostGIS point (lon, lat)
    valid_from      DATE            NOT NULL,
    valid_to        DATE,                       -- NULL = current record
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE,
    UNIQUE (location_key, version_number)
);

CREATE INDEX IF NOT EXISTS idx_dim_location_key_current
    ON dim_location (location_key) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_location_geom
    ON dim_location USING GIST (geom);

COMMENT ON TABLE dim_location IS
    'Location dimension (SCD Type 2). '
    'Seeded from known monitored locations; new version on reclassification. '
    'geom column enables PostGIS geo queries in Grafana Geomap panel.';


-- ---------------------------------------------------------------------------
-- dim_incident_category
-- Static lookup — seeded manually from observed TomTom category values.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_incident_category (
    category_sk         SERIAL          PRIMARY KEY,
    category_key        VARCHAR(50)     NOT NULL UNIQUE,  -- normalised slug e.g. 'jam'
    category_raw        VARCHAR(100)    NOT NULL UNIQUE,  -- raw TomTom string e.g. 'Jam'
    icon_category_id    SMALLINT,                         -- TomTom numeric id
    category_label_en   VARCHAR(100)    NOT NULL,
    category_label_ar   VARCHAR(100),
    category_group      VARCHAR(50)     NOT NULL,         -- Congestion / Closure / Hazard …
    severity_level      VARCHAR(20)     NOT NULL,         -- low / medium / high / critical
    severity_rank       SMALLINT        NOT NULL,         -- 1 (lowest) .. 4 (highest)
    causes_road_closure BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dim_incident_category IS
    'Incident category dimension — maps raw TomTom category strings to '
    'normalised keys, Arabic labels, and severity rankings.';


-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- fact_traffic_flow
-- Grain: one row per location per 15-minute window.
-- Source: silver_flow (1:1 row mapping after Silver aggregation).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_traffic_flow (
    flow_sk                         BIGSERIAL       PRIMARY KEY,

    -- Foreign keys
    date_key                        INT             NOT NULL
                                        REFERENCES dim_date (date_key),
    time_key                        SMALLINT        NOT NULL
                                        REFERENCES dim_time (time_key),
    location_sk                     INT
                                        REFERENCES dim_location (location_sk),

    -- Window bounds (UTC)
    window_start_utc                TIMESTAMPTZ     NOT NULL,
    window_end_utc                  TIMESTAMPTZ     NOT NULL,

    -- Road class (carried from Silver)
    frc_code                        VARCHAR(5),
    frc_label                       VARCHAR(50),

    -- Speed measures (km/h)
    avg_speed_kmh                   FLOAT,
    min_speed_kmh                   SMALLINT,
    max_speed_kmh                   SMALLINT,
    avg_free_flow_kmh               FLOAT,

    -- Congestion ratio measures (0.0 = standstill, 1.0 = free flow)
    avg_congestion_ratio            FLOAT,
    min_congestion_ratio            FLOAT,
    max_congestion_ratio            FLOAT,
    avg_congestion_severity_score   FLOAT,          -- (1 - ratio) * 100

    -- Travel time measures (seconds / minutes)
    avg_travel_time_seconds         FLOAT,
    avg_free_flow_travel_time_seconds FLOAT,
    avg_travel_time_delay_seconds   FLOAT,
    avg_travel_time_delay_minutes   FLOAT,

    -- Other derived
    avg_speed_drop_pct              FLOAT,

    -- Labels / flags
    congestion_level                VARCHAR(20),    -- free_flow / light / moderate / heavy / road_closed
    road_closed_flag                BOOLEAN,
    avg_confidence                  FLOAT,
    confidence_band                 VARCHAR(10),    -- high / medium / low

    -- Quality
    sample_count                    SMALLINT,

    -- Audit
    inserted_at                     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Unique constraint enables safe upsert on reprocessing
    UNIQUE (date_key, time_key, location_sk)
);

-- Grafana / Power BI query patterns
CREATE INDEX IF NOT EXISTS idx_ftf_window_start
    ON fact_traffic_flow (window_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_ftf_location
    ON fact_traffic_flow (location_sk);

CREATE INDEX IF NOT EXISTS idx_ftf_date_location
    ON fact_traffic_flow (date_key, location_sk);

CREATE INDEX IF NOT EXISTS idx_ftf_congestion_level
    ON fact_traffic_flow (congestion_level);

COMMENT ON TABLE fact_traffic_flow IS
    'Flow fact — grain: location × 15-min window. '
    'Loaded from silver_flow every 15 minutes via Spark foreachBatch.';


-- ---------------------------------------------------------------------------
-- fact_incidents
-- Grain: one row per incident per 15-minute ingestion window.
-- Source: silver_incidents (per-incident rows, not aggregated).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_incidents (
    incident_sk                 BIGSERIAL       PRIMARY KEY,

    -- Foreign keys
    date_key                    INT             NOT NULL
                                    REFERENCES dim_date (date_key),
    time_key                    SMALLINT        NOT NULL
                                    REFERENCES dim_time (time_key),
    location_sk                 INT
                                    REFERENCES dim_location (location_sk),
    category_sk                 INT
                                    REFERENCES dim_incident_category (category_sk),

    -- Ingestion window timestamp (UTC)
    window_start_utc            TIMESTAMPTZ     NOT NULL,

    -- Incident lifecycle timestamps (parsed from TomTom strings)
    incident_start_utc          TIMESTAMPTZ,
    incident_end_utc            TIMESTAMPTZ,
    incident_duration_minutes   FLOAT,

    -- Location detail (free-text from TomTom)
    from_location               TEXT,
    to_location                 TEXT,
    road_numbers                VARCHAR(100),
    lat                         DOUBLE PRECISION,
    lon                         DOUBLE PRECISION,
    has_known_location          BOOLEAN         NOT NULL DEFAULT FALSE,
    geom                        GEOMETRY(Point, 4326),  -- NULL when lat/lon missing

    -- Incident attributes
    magnitude                   SMALLINT,               -- 0..4
    magnitude_label             VARCHAR(20),            -- Unknown/Minor/Moderate/Major

    -- Impact measures
    delay_seconds               INT             NOT NULL DEFAULT 0,
    delay_minutes               FLOAT,
    length_meters               INT             NOT NULL DEFAULT 0,
    length_km                   FLOAT,

    -- Flags
    is_road_closure             BOOLEAN         NOT NULL DEFAULT FALSE,
    is_active_at_ingestion      BOOLEAN         NOT NULL DEFAULT TRUE,

    -- Audit
    inserted_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Grafana real-time dashboard: active incidents filtered by is_active_at_ingestion
CREATE INDEX IF NOT EXISTS idx_fi_active
    ON fact_incidents (is_active_at_ingestion, window_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fi_location
    ON fact_incidents (location_sk);

CREATE INDEX IF NOT EXISTS idx_fi_category
    ON fact_incidents (category_sk);

CREATE INDEX IF NOT EXISTS idx_fi_date_location
    ON fact_incidents (date_key, location_sk);

CREATE INDEX IF NOT EXISTS idx_fi_magnitude
    ON fact_incidents (magnitude);

CREATE INDEX IF NOT EXISTS idx_fi_geom
    ON fact_incidents USING GIST (geom);

COMMENT ON TABLE fact_incidents IS
    'Incident fact — grain: incident × 15-min ingestion window. '
    'Each active incident at poll time gets one row per window. '
    'Loaded from silver_incidents every 15 minutes via Spark foreachBatch.';


-- =============================================================================
-- HELPER: populate geom from lat/lon on insert/update
-- Keeps geom always in sync without requiring Spark to compute it.
-- =============================================================================

CREATE OR REPLACE FUNCTION sync_incident_geom()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    ELSE
        NEW.geom := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_incident_geom
    BEFORE INSERT OR UPDATE ON fact_incidents
    FOR EACH ROW EXECUTE FUNCTION sync_incident_geom();


CREATE OR REPLACE FUNCTION sync_location_geom()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    ELSE
        NEW.geom := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_location_geom
    BEFORE INSERT OR UPDATE ON dim_location
    FOR EACH ROW EXECUTE FUNCTION sync_location_geom();


-- =============================================================================
-- SEED: dim_time (96 rows — run once)
-- =============================================================================

INSERT INTO dim_time (time_key, hour, minute, period_label,
                      is_peak_am, is_peak_pm, is_off_peak, is_overnight)
SELECT
    (h * 4 + m / 15)::SMALLINT                         AS time_key,
    h::SMALLINT                                         AS hour,
    m::SMALLINT                                         AS minute,
    TO_CHAR(MAKE_TIME(h, m, 0), 'HH24:MI')             AS period_label,
    (h BETWEEN 7 AND 9)                                 AS is_peak_am,
    (h BETWEEN 16 AND 19)                               AS is_peak_pm,
    (h NOT BETWEEN 7 AND 9) AND (h NOT BETWEEN 16 AND 19)
        AND (h >= 5)                                    AS is_off_peak,
    (h < 5)                                             AS is_overnight
FROM
    generate_series(0, 23) AS h,
    generate_series(0, 45, 15) AS m
ON CONFLICT (time_key) DO NOTHING;


-- =============================================================================
-- SEED: dim_date  (2024-01-01 → 2026-12-31 — extend as needed)
-- Egyptian public holidays and Ramadan windows handled separately.
-- =============================================================================

INSERT INTO dim_date (
    date_key, full_date,
    year, quarter, month, month_name_en, month_name_ar,
    week_of_year, day_of_month, day_of_week,
    day_name_en, day_name_ar,
    is_weekend, season, day_type
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                         AS date_key,
    d::DATE                                             AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT                   AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT                   AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT                   AS month,
    TO_CHAR(d, 'Month')                                 AS month_name_en,
    -- Arabic month names via CASE
    CASE EXTRACT(MONTH FROM d)::INT
        WHEN 1  THEN 'يناير'   WHEN 2  THEN 'فبراير'
        WHEN 3  THEN 'مارس'    WHEN 4  THEN 'أبريل'
        WHEN 5  THEN 'مايو'    WHEN 6  THEN 'يونيو'
        WHEN 7  THEN 'يوليو'   WHEN 8  THEN 'أغسطس'
        WHEN 9  THEN 'سبتمبر'  WHEN 10 THEN 'أكتوبر'
        WHEN 11 THEN 'نوفمبر'  WHEN 12 THEN 'ديسمبر'
    END                                                 AS month_name_ar,
    EXTRACT(WEEK    FROM d)::SMALLINT                   AS week_of_year,
    EXTRACT(DAY     FROM d)::SMALLINT                   AS day_of_month,
    EXTRACT(ISODOW  FROM d)::SMALLINT                   AS day_of_week,   -- 1=Mon
    TO_CHAR(d, 'Day')                                   AS day_name_en,
    CASE EXTRACT(ISODOW FROM d)::INT
        WHEN 1 THEN 'الاثنين'   WHEN 2 THEN 'الثلاثاء'
        WHEN 3 THEN 'الأربعاء'  WHEN 4 THEN 'الخميس'
        WHEN 5 THEN 'الجمعة'    WHEN 6 THEN 'السبت'
        WHEN 7 THEN 'الأحد'
    END                                                 AS day_name_ar,
    EXTRACT(ISODOW FROM d) IN (5, 6)                   AS is_weekend,   -- Fri+Sat in Egypt
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5)  THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8)  THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9, 10,11) THEN 'Autumn'
        ELSE                                           'Winter'
    END                                                 AS season,
    CASE
        WHEN EXTRACT(ISODOW FROM d) IN (5, 6)    THEN 'Weekend'
        ELSE                                           'Weekday'
    END                                                 AS day_type
FROM generate_series(
    '2024-01-01'::DATE,
    '2026-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_key) DO NOTHING;


-- =============================================================================
-- Egyptian public holidays — update is_public_holiday + holiday_name + day_type
-- Add / extend this list as new official holidays are announced.
-- =============================================================================

UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'New Year''s Day',       day_type = 'Holiday' WHERE full_date = '2024-01-01';
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'January 25 Revolution',  day_type = 'Holiday' WHERE full_date IN ('2024-01-25','2025-01-25','2026-01-25');
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'Sinai Liberation Day',   day_type = 'Holiday' WHERE full_date IN ('2024-04-25','2025-04-25','2026-04-25');
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'Labour Day',             day_type = 'Holiday' WHERE full_date IN ('2024-05-01','2025-05-01','2026-05-01');
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = '30 June Revolution',     day_type = 'Holiday' WHERE full_date IN ('2024-06-30','2025-06-30','2026-06-30');
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'July 23 Revolution',     day_type = 'Holiday' WHERE full_date IN ('2024-07-23','2025-07-23','2026-07-23');
UPDATE dim_date SET is_public_holiday = TRUE, holiday_name = 'Armed Forces Day',       day_type = 'Holiday' WHERE full_date IN ('2024-10-06','2025-10-06','2026-10-06');
-- Eid / Ramadan: update manually each year after official announcement


-- =============================================================================
-- SEED: dim_incident_category (observed TomTom categories)
-- =============================================================================

INSERT INTO dim_incident_category (
    category_key, category_raw, icon_category_id,
    category_label_en, category_label_ar,
    category_group, severity_level, severity_rank, causes_road_closure
) VALUES
    ('jam',          'Jam',          1, 'Traffic Jam',       'ازدحام مروري',       'Congestion', 'medium',   2, FALSE),
    ('road_closed',  'Road closed',  6, 'Road Closure',      'إغلاق طريق',         'Closure',    'critical', 4, TRUE),
    ('accident',     'Accident',     3, 'Accident',          'حادث',               'Hazard',     'high',     3, FALSE),
    ('road_works',   'Road works',   8, 'Road Works',        'أعمال طريق',         'Hazard',     'medium',   2, FALSE),
    ('dangerous_conditions', 'Dangerous conditions', 9,
                                       'Dangerous Conditions','ظروف خطرة',         'Hazard',     'high',     3, FALSE),
    ('car_stopped',  'Car stopped',  2, 'Stopped Vehicle',   'مركبة متوقفة',       'Hazard',     'low',      1, FALSE),
    ('flooding',     'Flooding',     5, 'Flooding',          'فيضانات',            'Hazard',     'critical', 4, TRUE),
    ('broken_down',  'Broken down vehicle', 10,
                                       'Broken Down Vehicle','مركبة معطلة',        'Hazard',     'low',      1, FALSE),
    ('unknown',      'Unknown',      0, 'Unknown',           'غير معروف',          'Other',      'low',      1, FALSE)
ON CONFLICT (category_raw) DO NOTHING;


-- =============================================================================
-- SAMPLE: dim_location seed (monitored locations — extend as needed)
-- geom is auto-populated by the trigger above.
-- =============================================================================

INSERT INTO dim_location (
    location_key, version_number, location_name,
    city, governorate, area_zone, road_type, frc, frc_label,
    lat, lon, valid_from, is_current
) VALUES
    ('tahrir_square',         1, 'Tahrir Square',          'cairo',      'Cairo',      'Downtown Cairo',    'arterial', 'FRC2', 'Secondary Road',   30.0444,  31.2358, '2024-01-01', TRUE),
    ('cairo_airport',         1, 'Cairo Airport',          'cairo',      'Cairo',      'Airport Corridor',  'highway',  'FRC1', 'Major Road',        30.1219,  31.4056, '2024-01-01', TRUE),
    ('ring_road_north',       1, 'Cairo Ring Road North',  'cairo',      'Cairo',      'Ring Road',         'highway',  'FRC0', 'Motorway / Freeway',30.1200,  31.3100, '2024-01-01', TRUE),
    ('ring_road_east',        1, 'Cairo Ring Road East',   'cairo',      'Cairo',      'Ring Road',         'highway',  'FRC0', 'Motorway / Freeway',30.0700,  31.4000, '2024-01-01', TRUE),
    ('ring_road_south',       1, 'Cairo Ring Road South',  'cairo',      'Cairo',      'Ring Road',         'highway',  'FRC0', 'Motorway / Freeway',29.9800,  31.3100, '2024-01-01', TRUE),
    ('ring_road_west',        1, 'Cairo Ring Road West',   'cairo',      'Cairo',      'Ring Road',         'highway',  'FRC0', 'Motorway / Freeway',30.0500,  31.1800, '2024-01-01', TRUE),
    ('6th_october_bridge',    1, '6th October Bridge',     'cairo',      'Cairo',      'Downtown Cairo',    'highway',  'FRC1', 'Major Road',        30.0520,  31.2280, '2024-01-01', TRUE),
    ('new_admin_capital',     1, 'New Administrative Capital','cairo',   'Cairo',      'East Cairo',        'highway',  'FRC1', 'Major Road',        30.0167,  31.7333, '2024-01-01', TRUE),
    ('giza_square',           1, 'Giza Square',            'cairo',      'Giza',       'Giza',              'arterial', 'FRC2', 'Secondary Road',    30.0082,  31.2113, '2024-01-01', TRUE),
    ('giza_pyramids',         1, 'Giza Pyramids Road',     'cairo',      'Giza',       'Giza',              'arterial', 'FRC2', 'Secondary Road',    29.9769,  31.1313, '2024-01-01', TRUE),
    ('salah_salem_north',     1, 'Salah Salem North',      'cairo',      'Cairo',      'Downtown Cairo',    'arterial', 'FRC2', 'Secondary Road',    30.0650,  31.2800, '2024-01-01', TRUE),
    ('salah_salem_south',     1, 'Salah Salem South',      'cairo',      'Cairo',      'Downtown Cairo',    'arterial', 'FRC2', 'Secondary Road',    30.0350,  31.2700, '2024-01-01', TRUE),
    ('corniche_downtown',     1, 'Nile Corniche Downtown', 'cairo',      'Cairo',      'Downtown Cairo',    'arterial', 'FRC2', 'Secondary Road',    30.0450,  31.2280, '2024-01-01', TRUE),
    ('ramses_square',         1, 'Ramses Square',          'cairo',      'Cairo',      'Downtown Cairo',    'arterial', 'FRC2', 'Secondary Road',    30.0626,  31.2497, '2024-01-01', TRUE),
    ('new_cairo_90th',        1, 'New Cairo 90th Street',  'cairo',      'Cairo',      'East Cairo',        'arterial', 'FRC2', 'Secondary Road',    30.0200,  31.4700, '2024-01-01', TRUE),
    ('mehwar_north',          1, 'Al Mehwar North',        'cairo',      'Giza',       '6th of October',    'highway',  'FRC1', 'Major Road',        30.0800,  31.0600, '2024-01-01', TRUE),
    ('alex_corniche_east',    1, 'Alexandria Corniche East','alexandria', 'Alexandria', 'Eastern Corniche',  'arterial', 'FRC2', 'Secondary Road',    31.2321,  29.9553, '2024-01-01', TRUE),
    ('alex_corniche_west',    1, 'Alexandria Corniche West','alexandria', 'Alexandria', 'Western Corniche',  'arterial', 'FRC2', 'Secondary Road',    31.2100,  29.9000, '2024-01-01', TRUE),
    ('alex_victoria_square',  1, 'Alexandria Victoria Square','alexandria','Alexandria','Central Alexandria','arterial', 'FRC2', 'Secondary Road',    31.2156,  29.9553, '2024-01-01', TRUE),
    ('alex_port_area',        1, 'Alexandria Port Area',   'alexandria', 'Alexandria', 'Port',              'arterial', 'FRC2', 'Secondary Road',    31.2001,  29.8952, '2024-01-01', TRUE),
    ('alex_montaza',          1, 'Alexandria Montaza',     'alexandria', 'Alexandria', 'Eastern Alexandria','local',    'FRC3', 'Connecting Road',   31.2846,  30.0191, '2024-01-01', TRUE),
    ('alex_abu_qir_road',     1, 'Alexandria Abu Qir Road','alexandria', 'Alexandria', 'Eastern Alexandria','highway',  'FRC1', 'Major Road',        31.3200,  30.0700, '2024-01-01', TRUE),
    ('alexandria_corniche',   1, 'Alexandria Corniche',    'alexandria', 'Alexandria', 'Central Corniche',  'arterial', 'FRC2', 'Secondary Road',    31.2200,  29.9300, '2024-01-01', TRUE)
ON CONFLICT (location_key, version_number) DO NOTHING;
