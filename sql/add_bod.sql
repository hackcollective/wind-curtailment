--Neat trick to get these: dump dataframe to sql, then use the
--sqlite3 CLI and call .schema tablename

DROP TABLE IF EXISTS bod;

CREATE TABLE bod (
        "bmUnitID" TEXT,
        local_datetime TIMESTAMP,
        "recordType" TEXT,
        "bMUnitType" TEXT,
        "leadPartyName" TEXT,
        "ngcBMUnitName" TEXT,
        "settlementDate" TEXT,
        "settlementPeriod" TEXT,
        "bidOfferPairNumber" REAL,
        "timeFrom" DATETIME,
        "bidOfferLevelFrom" REAL,
        "timeTo" DATETIME,
        "bidOfferLevelTo" REAL,
        "bidPrice" REAL,
        "offerPrice" REAL,
        "activeFlag" TEXT,

    PRIMARY KEY("bmUnitID", "timeFrom", "timeTo","bidOfferPairNumber")

    );

CREATE INDEX ix_bod_unit ON bod ("bmUnitID");
CREATE INDEX ix_bod_time ON bod ("timeFrom", "timeTo");
