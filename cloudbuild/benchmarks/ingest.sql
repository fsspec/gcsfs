-- 1. Ensure the history table exists with metadata columns
CREATE TABLE IF NOT EXISTS `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.history`
(
  run_date DATE,
  build_id STRING,
  run_timestamp TIMESTAMP,
  source_uri STRING
)
PARTITION BY run_date;

-- 2. Dynamically add missing columns to history
DECLARE alter_stmt STRING;
SET alter_stmt = (
  SELECT
    CONCAT("ALTER TABLE `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.history` ",
           STRING_AGG(CONCAT("ADD COLUMN `", column_name, "` ", data_type), ", "))
  FROM `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'staging'
    AND column_name NOT IN (
      SELECT column_name FROM `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = 'history'
    )
);

IF alter_stmt IS NOT NULL THEN
  EXECUTE IMMEDIATE alter_stmt;
END IF;

-- 3. Perform idempotent ingestion
INSERT INTO `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.history`
SELECT
  PARSE_DATE('%d%m%Y', REGEXP_EXTRACT(_FILE_NAME, r'/(\d{8})/')) as run_date,
  REGEXP_EXTRACT(_FILE_NAME, r'/([0-9a-fA-F-]{36})/') as build_id,
  PARSE_TIMESTAMP('%d%m%Y-%H%M%S', REGEXP_EXTRACT(_FILE_NAME, r'/(\d{8}-\d{6})/')) as run_timestamp,
  _FILE_NAME as source_uri,
  *
FROM `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.staging`
WHERE _FILE_NAME NOT IN (SELECT DISTINCT source_uri FROM `gcs-aiml-clients-testing-101.gcsfs_microbenchmarks.history`);
