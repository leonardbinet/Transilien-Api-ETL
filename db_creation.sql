/*
date 23/05/2012 13:12
num String OUI Le numéro du train 148407
miss String OUI Code mission du train SARA
term String NON Terminus du train 87393843
etat String
*/
CREATE TABLE departures
(
    id INT NOT NULL AUTO_INCREMENT,
    station VARCHAR(10),
    request_date VARCHAR(20),
    date VARCHAR(80),
    num VARCHAR(10),
    miss VARCHAR(5),
    term VARCHAR(10),
    etat VARCHAR(40),
    PRIMARY KEY (id)
)
