create external table customer_landing(
    birthDay string,
    customerName string,
    email string,
    lastUpdateDate bigint,
    phone string,
    registrationDate bigint, 
    serialNumber string,
    shareWithFriendsAsOfDate bigint,
    shareWithPublicAsOfDate bigint,
    shareWithResearchAsOfDate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://agil-final-project/landing/customer/';
