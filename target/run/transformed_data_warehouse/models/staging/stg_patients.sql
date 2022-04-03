

  create or replace view `bachelor-thesis-344103`.`optimized_data_warehouse`.`stg_patients`
  OPTIONS()
  as 

select * from `bachelor-thesis-344103`.`raw_data_warehouse`.`patients`
limit 100;

