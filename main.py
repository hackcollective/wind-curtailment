import datetime

import pandas as pd
import streamlit as st
from streamlit_extras.metric_cards import style_metric_cards

from lib.constants import (
    MW_TO_TW,
    MW_TO_GW,
    GBP_TO_MGBP,
    GBP_TO_KGBP,
    MW_30m_TO_MWH,
    G_CO2E_PER_KWH_GAS,
    G_TO_TONNE,
    MWH_TO_KWH,
)
from lib.curtailment import MINUTES_TO_HOURS
from lib.gcp_db_utils import read_data
from lib.plot import make_time_series_plot, limit_plot_size

MIN_DATE = pd.to_datetime("2021-01-01")
MAX_DATE = pd.to_datetime("2025-01-01")

st.set_page_config(page_title="UK Wind Curtailment Monitor", page_icon="./static/favicon.png")


@st.cache
def get_data(current_hour: str) -> pd.DataFrame:
    """
    Use the current hour as the key for the cache, so that when the hour changes, we
    will pull a new version of the data, but we aren't constantly reading
    from the database within each hour
    """

    return read_data(start_time=MIN_DATE, end_time=MAX_DATE)


@st.cache
def filter_data(df, start_date, end_date):
    return df[(df["time"] >= pd.to_datetime(start_date)) & (df["time"] <= pd.to_datetime(end_date))]


@st.cache
def transform_data(df: pd.DataFrame):
    if "cost_gbp" not in df.columns:
        df["cost_gbp"] = 99.99999

    filtered_df = filter_data(df, MIN_DATE, MAX_DATE).copy()
    filtered_df["month_and_year"] = filtered_df["time"].dt.month_name() + " " + filtered_df["time"].dt.year.astype(str)
    filtered_df["year"] = filtered_df["time"].dt.year.astype(str)
    total_curtailment = filtered_df["delta_mw"].sum() * MINUTES_TO_HOURS

    return filtered_df, total_curtailment


def write_summary_box(df: pd.DataFrame, energy_units="GWh", price_units="M"):

    if energy_units == "GWh":
        energy_converter = MW_TO_GW
        co2_units = "kt"
    elif energy_units == "TWh":
        energy_converter = MW_TO_TW
        co2_units = "mt"
    else:
        raise ValueError

    curtailment = df["delta_mw"].sum() * energy_converter * MW_30m_TO_MWH

    if price_units == "M":
        price_converter = GBP_TO_MGBP
    elif price_units == "K":
        price_converter = GBP_TO_KGBP
    else:
        raise ValueError

    turndown_curtailment_cost = df["cost_gbp"].sum() * price_converter
    turnup_curtailment_cost = df["turnup_cost_gbp"].sum() * price_converter

    total_curtailment_cost = turndown_curtailment_cost + turnup_curtailment_cost

    total_emissions_tonnes = curtailment * MWH_TO_KWH * G_CO2E_PER_KWH_GAS * G_TO_TONNE

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label=f"Curtailed Wind", value=f"{curtailment:,.1f} {energy_units}")
    with col2:
        st.metric(label=f"Cost", value=f"£{total_curtailment_cost:,.1f}{price_units}")
    with col3:
        st.metric(label=f"CO2 Emissions", value=f"{total_emissions_tonnes:,.1f}{co2_units}")

    style_metric_cards(border_left_color="rgb(250,100,50)")


def write_yearly_plot(df: pd.DataFrame, year: str) -> None:

    # this codes the year and month into 'YYYY0MM', which can be used to sort
    year_df = df[df["year"] == year]
    year_df["year_month_idx"] = 100 * year_df["time"].dt.year + year_df["time"].dt.month
    year_df_mean = year_df.groupby("month_and_year").mean()
    year_df = year_df.groupby("month_and_year").sum()
    year_df["month_and_year"] = year_df.index
    year_df["time"] = year_df["month_and_year"]
    year_df["year_month_idx"] = year_df_mean["year_month_idx"]
    year_df = year_df.sort_values(by=["year_month_idx"])

    st.header(f"Total Wind Curtailment for {year}")
    write_summary_box(year_df, energy_units="TWh", price_units="M")
    fig = make_time_series_plot(year_df.copy(), mw_or_mwh="mwh")

    st.plotly_chart(fig)


def write_all_year_plot(df: pd.DataFrame) -> None:

    # format data
    year_df = df.copy()
    year_df["time"] = year_df["year"]
    year_df = year_df.groupby("time").sum()
    year_df["time"] = year_df.index

    # plot the data
    st.header(f"Total Wind Curtailment")
    write_summary_box(year_df, energy_units="TWh", price_units="M")
    fig = make_time_series_plot(year_df.copy(), mw_or_mwh="mwh")

    # make sure only the years show on the plot, not for example 2019.5
    years = df["year"].unique()
    fig.update_layout(
        xaxis=dict(
            tickmode='array',  # change 1
            tickvals=years,))
    st.plotly_chart(fig)


def write_monthly_plot(df: pd.DataFrame, month_and_year: str) -> None:
    monthly_df = df[df["month_and_year"] == month_and_year]
    monthly_df["time"] = monthly_df["time"].dt.date
    monthly_df = monthly_df.groupby("time").sum()
    monthly_df["time"] = monthly_df.index

    st.header(f"Wind Curtailment for {month_and_year}")
    write_summary_box(monthly_df, energy_units="GWh", price_units="M")
    fig = make_time_series_plot(monthly_df.copy(), mw_or_mwh="mwh")
    st.plotly_chart(fig)


def write_daily_plot(df: pd.DataFrame, select_date: datetime.date) -> None:
    daily_df = df[df["time"].dt.date == select_date]
    daily_df = daily_df.groupby("time").sum()
    daily_df["time"] = daily_df.index

    st.header(f"Wind Curtailment for {select_date.strftime('%d %B %Y')}")
    write_summary_box(daily_df, energy_units="GWh", price_units="K")

    fig = make_time_series_plot(daily_df.copy())
    st.plotly_chart(fig)


current_hour = datetime.datetime.now().strftime("%d/%m/%Y %H")
df = get_data(current_hour=current_hour)

filtered_df, total_curtailment = transform_data(df)

st.session_state.today_date = pd.to_datetime("today").date()
INITIAL_END_DATE = pd.to_datetime("2023-01-01")

st.title("UK Wind Curtailment Monitor")
st.info(
    "Explore the wind power that the UK is "
    "discarding due to transmission constraints. Select a date to "
    "see the data for that day and month."
    "\n\n"
    "See [this post](https://archy.deberker.com/the-uk-is-wasting-a-lot-of-wind-power/) for discussion and "
    "notes on our methodology [here](https://wooden-knee-d53.notion.site/UK-Wind-Curtailment-Monitor-Methodology-71475d0b7cfd4edb97d6397b358f4118)."
)
select_date = st.date_input("Select Date", min_value=MIN_DATE, max_value=MAX_DATE, value=st.session_state.today_date)
month_and_year = pd.to_datetime(select_date).month_name() + " " + str(pd.to_datetime(select_date).year)
year = str(pd.to_datetime(select_date).year)

limit_plot_size()

write_daily_plot(filtered_df, select_date)
write_monthly_plot(filtered_df, month_and_year)
write_yearly_plot(filtered_df, year)
write_all_year_plot(filtered_df)


csv = df.to_csv().encode("utf-8")

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=f"wind_curtailment_{current_hour}.csv",
    mime="text/csv",
)

st.markdown("Please reference [![DOI](https://zenodo.org/badge/499136108.svg)](https://doi.org/10.5281/zenodo.13936552)")

st.markdown("<div style='text-align: center; margin-top: 50px; color: rgba(40,80,80,0.9)'> "
            "<p>🛠 Made by <a href='https://www.linkedin.com/in/peter-dudfield-b379b7a6/'>Peter Dudfield </a>"
            "and <a href='https://www.linkedin.com/in/archy-de-berker/'>Archy de Berker</a> </p>"
            "<p> Please <a href='mailto:archy.deberker@gmail.com,peter@openclimatefix.org'> get in touch </a> with questions or feedback. </p></div>", unsafe_allow_html=True)
