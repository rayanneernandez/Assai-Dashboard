CREATE TABLE IF NOT EXISTS public.users (
  id SERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.dashboard_daily (
  day DATE NOT NULL,
  store_id VARCHAR(64),
  total_visitors INT NOT NULL,
  male INT NOT NULL,
  female INT NOT NULL,
  avg_age_sum FLOAT8 NOT NULL,
  avg_age_count INT NOT NULL,
  age_18_25 INT NOT NULL,
  age_26_35 INT NOT NULL,
  age_36_45 INT NOT NULL,
  age_46_60 INT NOT NULL,
  age_60_plus INT NOT NULL,
  monday INT NOT NULL,
  tuesday INT NOT NULL,
  wednesday INT NOT NULL,
  thursday INT NOT NULL,
  friday INT NOT NULL,
  saturday INT NOT NULL,
  sunday INT NOT NULL,
  PRIMARY KEY (day, store_id)
);
ALTER TABLE public.dashboard_daily ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE TABLE IF NOT EXISTS public.dashboard_hourly (
  day DATE NOT NULL,
  store_id VARCHAR(64) NOT NULL,
  hour SMALLINT NOT NULL,
  total INT NOT NULL,
  male INT NOT NULL,
  female INT NOT NULL,
  PRIMARY KEY (day, store_id, hour)
);

CREATE TABLE IF NOT EXISTS public.visitors (
  visitor_id TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  store_id VARCHAR(64) NOT NULL,
  store_name TEXT,
  gender CHAR(1),
  age INT,
  day_of_week TEXT,
  smile BOOLEAN,
  PRIMARY KEY (visitor_id, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON public.visitors (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_visitors_store ON public.visitors (store_id);
