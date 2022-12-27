from lib.data.utils import client


def call_sbp_api(start_date: str, end_date: str):
    return client.get_DERSYSDATA(start_date=start_date, end_date=end_date)