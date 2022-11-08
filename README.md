# wind-curtailment

# Data
We hit the Elexon API to get data. See `scripts/fetch_data.py. This is saved to an SQLite DB. See details in that
script.

Raw data is also saved as `feather` files to './data/PHYBM/raw'.

# Analysis
Run `scripts/calculate_curtailment.py` to run the analysis against the SQLite DB.

## Notebooks
There's some old analysis in `scripts` and `notebooks/curtailment.ipynb`,
mostly useful for identifying the right day to focus on.

Prototype is in `notebooks/bidoffer.ipynb`.


Might need to run
`export PYTHONPATH=${PYTHONPATH}:/lib`
to get the lib in to your python path