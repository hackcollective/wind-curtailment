DROP TABLE IF EXISTS sbp;

CREATE TABLE sbp
(
    "time" TIMESTAMP,
    "system_buy_price" REAL,
    "created_utc" TIMESTAMP DEFAULT NOW()
    PRIMARY KEY("time")

);
