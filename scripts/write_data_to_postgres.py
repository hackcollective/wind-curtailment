import sys

from lib.gcp_db_utils import write_data, load_data

sys.path.append("/src")

from lib.constants import BASE_DIR

if __name__ == "__main__":
    path = BASE_DIR / "data/outputs/results-2022-01-01-2022-10-01.csv"
    df = load_data(path)
    write_data(df)
