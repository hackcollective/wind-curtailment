from lib.gcp_db_utils import read_data

df = read_data('2022-01-01', '2022-02-11')
df.to_csv("local_data_sample.csv")
