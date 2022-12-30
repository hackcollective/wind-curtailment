DROP TABLE IF EXISTS curtailment;

CREATE TABLE curtailment
(
    "time" TIMESTAMP,
    "level_fpn" REAL,
    "level_boal" REAL,
    "level_after_boal" REAL,
    "delta_mw" REAL,
    "cost_gbp" REAL,
    "created_utc" TIMESTAMP DEFAULT NOW()

    PRIMARY KEY("time")

);
