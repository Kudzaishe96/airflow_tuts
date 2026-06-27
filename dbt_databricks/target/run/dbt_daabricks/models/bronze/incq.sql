
  
    
        create or replace table `docker_dbt`.`bronze`.`incq`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      Select * from docker_dbt.staging.The2014Inc
  