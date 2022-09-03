--Neat trick to get these: dump dataframe to sql, then use the
--sqlite3 CLI and call .schema tablename

DROP TABLE IF EXISTS fpn;
DROP TABLE IF EXISTS boal;

CREATE TABLE fpn (
        "unit" TEXT,
        local_datetime TIMESTAMP,
        "recordType" TEXT,
        "bMUnitType" TEXT,
        "leadPartyName" TEXT,
        "ngcBMUnitName" TEXT,
        "settlementDate" TEXT,
        "settlementPeriod" TEXT,
        "timeFrom" DATETIME,
        "levelFrom" REAL,
        "timeTo" DATETIME,
        "levelTo" REAL,
        "activeFlag" TEXT,
        "Fuel Type" TEXT,

    PRIMARY KEY("unit", "timeFrom", "timeTo")

    );

CREATE INDEX ix_fpn_unit ON fpn ("unit");
CREATE INDEX ix_fpn_time ON fpn ("timeFrom", "timeTo");


CREATE TABLE boal (
    "unit" TEXT,
    local_datetime TIMESTAMP,
    "recordType" TEXT,
    "bMUnitType" TEXT,
    "leadPartyName" TEXT,
    "ngcBMUnitName" TEXT,
    "settlementDate" TEXT,
    "settlementPeriod" TEXT,
    "timeFrom" DATETIME,
    "timeTo" DATETIME,
    "activeFlag" TEXT,
    "Accept ID" TEXT,
    "Accept Time" TEXT,
    "deemedBidOfferFlag" TEXT,
    "soFlag" TEXT,
    "storProviderFlag" TEXT,
    "rrInstructionFlag" TEXT,
    "rrScheduleFlag" TEXT,
    "levelFrom" REAL,
    "levelTo" REAL,
    "Fuel Type" TEXT,

    PRIMARY KEY("unit", "timeFrom", "timeTo", "Accept ID")
);

CREATE INDEX ix_boal_unit ON boal ("unit");
CREATE INDEX ix_boal_time ON fpn ("timeFrom", "timeTo");