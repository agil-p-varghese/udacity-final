create external table step_trainer_landing(
    distanceFromObject bigint,
    sensorReadingTime bigint,
    serialNumber string 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://agil-final-project/landing/step_trainer/';
