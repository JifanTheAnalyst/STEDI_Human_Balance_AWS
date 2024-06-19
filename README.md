# STEDI_Human_Balance_AWS
## Summary
AWS glue jobs are use for ETL with files in s3 to build a data lakehouse solution for sensor data that trains a machine learning model.
## Tables
- Customer_landing, accelerometer_landing and step_trainer_landing are original files
- Customer_trusted: filter out customers who agree to share
- Accelerometer_trusted: filter out customers who agree to share
- Customer_curated: filter out customers who agree to share and joined with Accelerometer_trusted 
- Step_trainer_trusted: filter out customers who agree to share and have the serial number
- Machine_leanring_curated: joined Customer_curated and Step_trainer_trusted
