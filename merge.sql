CREATE TABLE IF NOT EXISTS $table_id (
  id STRING,
  name STRING,
  birth_date STRING,
  occupation STRING,
  gender STRING
);

MERGE `$table_id` P
USING `$stage_table_id` N
ON P.id = N.id
WHEN MATCHED THEN
  UPDATE SET name = N.name, birth_date = N.birth_date, occupation = N.occupation, gender = N.gender
WHEN NOT MATCHED THEN
  INSERT (id, name, birth_date, occupation, gender) VALUES(id, name, birth_date, occupation, gender)
