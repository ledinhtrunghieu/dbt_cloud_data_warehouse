

  create or replace table `bachelor-thesis-344103`.`optimized_data_warehouse`.`person`
  
  
  OPTIONS()
  as (
    

with p as (
  select *
  from `bachelor-thesis-344103`.`raw_data_warehouse`.`patients`
)
SELECT
  ROW_NUMBER() OVER( ORDER BY p.id) as person_id,
  case
    upper(p.gender)
    when 'M' then 8507
    when 'F' then 8532
  end as gender_concept_id,
  EXTRACT(ISOYEAR FROM  CAST (p.birthdate AS TIMESTAMP)) as year_of_birth,
  EXTRACT(MONTH FROM  CAST (p.birthdate AS TIMESTAMP)) as month_of_birth,
  EXTRACT(DAY FROM  CAST (p.birthdate AS TIMESTAMP)) as day_of_birth,
  CAST (p.birthdate AS TIMESTAMP) as birth_datetime,
  case
    upper(p.race)
    when 'WHITE' then 8527
    when 'BLACK' then 8516
    when 'ASIAN' then 8515
    else 0
  end as race_concept_id,
  case
    when upper(p.race) = 'HISPANIC' then 38003563
    else 0
  end as ethnicity_concept_id,
  0 as location_id,
  0 as provider_id,
  0 as care_site_id,
  p.id as person_source_value,
  p.gender as gender_source_value,
  0 as gender_source_concept_id,
  p.race as race_source_value,
  0 as race_source_concept_id,
  p.ethnicity as ethnicity_souce_value,
  0 as ethnicity_souce_concept_id
from p
  );
  