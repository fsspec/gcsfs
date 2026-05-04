-- 1. Variable declarations must be at the top
DECLARE alter_stmt STRING;

-- 2. Ensure the history table exists with metadata columns
CREATE TABLE IF NOT EXISTS `@PROJECT_ID@.@DATASET_NAME@.history`
(
  run_date DATE,
  build_id STRING,
  run_timestamp TIMESTAMP,
  source_uri STRING,
  branch_name STRING
)
PARTITION BY run_date;

-- 3. Dynamically find new columns in staging that are missing from history
SET alter_stmt = (
  SELECT
    CONCAT("ALTER TABLE `@PROJECT_ID@.@DATASET_NAME@.history` ",
           STRING_AGG(CONCAT("ADD COLUMN `", column_name, "` ", data_type), ", "))
  FROM `@PROJECT_ID@.@DATASET_NAME@.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'staging'
    AND column_name NOT IN (
      SELECT column_name
      FROM `@PROJECT_ID@.@DATASET_NAME@.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = 'history'
    )
);

-- 4. Execute the schema update only if new columns were found
IF alter_stmt IS NOT NULL THEN
  EXECUTE IMMEDIATE alter_stmt;
END IF;

-- 5. Perform the idempotent ingestion
INSERT INTO `@PROJECT_ID@.@DATASET_NAME@.history`
SELECT
  PARSE_DATE('%d%m%Y', REGEXP_EXTRACT(_FILE_NAME, r'/(\d{8})/')) as run_date,
  REGEXP_EXTRACT(_FILE_NAME, r'/([0-9a-fA-F-]{36})/') as build_id,
  PARSE_TIMESTAMP('%d%m%Y-%H%M%S', REGEXP_EXTRACT(_FILE_NAME, r'/(\d{8}-\d{6})/')) as run_timestamp,
  _FILE_NAME as source_uri,
  REGEXP_EXTRACT(_FILE_NAME, r'/branch=([^/]+)/') as branch_name,
  *
FROM `@PROJECT_ID@.@DATASET_NAME@.staging`
WHERE _FILE_NAME NOT IN (
  SELECT DISTINCT source_uri
  FROM `@PROJECT_ID@.@DATASET_NAME@.history`
);
