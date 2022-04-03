

  create or replace view `bachelor-thesis-344103`.`optimized_data_warehouse`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `bachelor-thesis-344103`.`optimized_data_warehouse`.`my_first_dbt_model`
where id = 1;

