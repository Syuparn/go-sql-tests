USE practice;

DROP TABLE IF EXISTS user;

CREATE TABLE user
(
    id          VARCHAR(26) PRIMARY KEY,
    name        VARCHAR(40) NOT NULL UNIQUE,
    age         INT
);
