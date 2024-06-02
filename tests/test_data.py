import tempfile
from pathlib import Path

from lib.data.fetch_bod_data import fetch_bod_data


def test_fetch_bod_data():

    start_date = "2024-04-01"
    end_date = "2024-04-01 08:00"

    unit_ids = ["T_ABRBO-1", "E_ABRTW-1", "T_ACHRW-1"]

    with tempfile.TemporaryDirectory() as temp_dir:
        save_dir = Path(temp_dir)

        df = fetch_bod_data(
            start_date=start_date,
            end_date=end_date,
            save_dir=save_dir,
            cache=True,
            unit_ids=unit_ids,
        )

    record_types = df["recordType"].unique()
    assert len(record_types) == 1
    assert record_types[0] == "BOD"
    # should be just BOD

    # Not sure this is working right now
    assert len(df) == 96
    assert "recordType" in df.columns
    assert "bmUnitID" in df.columns
    assert "timeFrom" in df.columns
    assert "timeTo" in df.columns
    assert "bidPrice" in df.columns
    assert "offerPrice" in df.columns
