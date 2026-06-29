BEGIN
  DECLARE alter_stmt STRING;
  DECLARE columns_list STRING;
  DECLARE insert_query STRING;

  CREATE TABLE IF NOT EXISTS `@PROJECT_ID@.@DATASET_NAME@.history`
  (
    run_date DATE,
    build_id STRING,
    run_timestamp TIMESTAMP,
    source_uri STRING,
    branch_name STRING
  )
  PARTITION BY run_date;

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

  IF alter_stmt IS NOT NULL THEN
    EXECUTE IMMEDIATE alter_stmt;
  END IF;

  -- Exclude the columns history derives from _FILE_NAME (run_date, build_id,
  -- run_timestamp, source_uri, branch_name). They are inserted explicitly
  -- below; if a future metric column ever collided with one of these names it
  -- would otherwise appear twice in the INSERT column list and fail.
  SET columns_list = (
    SELECT STRING_AGG(CONCAT("`", column_name, "`"), ", " ORDER BY column_name)
    FROM `@PROJECT_ID@.@DATASET_NAME@.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'staging'
      AND column_name NOT IN ('run_date', 'build_id', 'run_timestamp',
                              'source_uri', 'branch_name')
  );

  SET insert_query = CONCAT(
    "INSERT INTO `@PROJECT_ID@.@DATASET_NAME@.history` (run_date, build_id, run_timestamp, source_uri, branch_name, ",
    columns_list,
    ") SELECT PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_FILE_NAME, r'/(\\d{8})/')) as run_date, ",
    "REGEXP_EXTRACT(_FILE_NAME, r'/buildid-([^/]+)/') as build_id, ",
    "PARSE_TIMESTAMP('%Y%m%d-%H%M%S', REGEXP_EXTRACT(_FILE_NAME, r'/(\\d{8}-\\d{6})\\.csv')) as run_timestamp, ",
    "_FILE_NAME as source_uri, ",
    "REGEXP_EXTRACT(_FILE_NAME, r'/branch=([^/]+)/') as branch_name, ",
    columns_list,
    " FROM `@PROJECT_ID@.@DATASET_NAME@.staging` s WHERE NOT EXISTS (SELECT 1 FROM `@PROJECT_ID@.@DATASET_NAME@.history` h WHERE h.source_uri = s._FILE_NAME)"
  );

  EXECUTE IMMEDIATE insert_query;
END;
