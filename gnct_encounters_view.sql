CREATE TABLE IF NOT EXISTS gnctdb.encounters_view
  WITH (external_location='s3://ac850692499868-gnc-test/athena/Viz/encounters_view') AS
  
  SELECT DISTINCT
  p.last AS patient_name,
  p.id as patient_id,
  p.gender,
  cast (p.birthdate AS date) AS birthdate,
  p.marital as marital_status,
  p.deathdate AS deceaseddatetime,
  p.race,
  p.ethnicity,
  p.county,
  p.lat,
  p.lon,
  enc.id as encounter_id,
  enc.start AS start_enc,
  enc.stop AS end_enc,
  enc.description as encounter_type,
  cond.description as condition
  
  FROM
  gnctdb.patients p
  
  FULL OUTER JOIN gnctdb.encounters enc
  ON enc.patient = p.id
  FULL OUTER JOIN gnctdb.conditions cond
  ON cond.encounter = enc.id