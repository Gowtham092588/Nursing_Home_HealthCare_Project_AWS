import streamlit as st
import pandas as pd
import plotly.express as px
from athenautil import run_athena

st.set_page_config(page_title="HealthCare Analytics DashBoard", layout="wide")

st.title("🏥 Nursing Home Healthcare Dashboard")

# ---------------------------------------------------------
# Cached query functions
# ---------------------------------------------------------


@st.cache_data(ttl=300)
def load_care_hours_per_bed() -> pd.DataFrame:
    query = """
        SELECT
            provider_id,
            work_date,
            total_rn_hours,
            total_lpn_hours,
            total_cna_hours,
            total_care_hours,
            certified_beds,
            total_care_hours_per_certified_bed
        FROM total_care_hours_per_certified_bed
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_nursing_hours_per_patient() -> pd.DataFrame:
    query = """
        SELECT
            provider_id,
            provider_name,
            state,
            work_date,
            total_rn_hours,
            total_lpn_hours,
            total_cna_hours,
            total_patients,
            total_nursing_hours,
            nurse_to_patient_ratio
        FROM avg_nurse_hours_to_patient_hospital
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_top_providers() -> pd.DataFrame:
    query = """
        SELECT
            provider_id,
            provider_name,
            state,
            avg_residents_per_day
        FROM top_ten_providers_by_avg_residents
        ORDER BY avg_residents_per_day DESC
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_staffing_ratio() -> pd.DataFrame:
    query = """
        SELECT
            provider_id,
            provider_name,
            state,
            work_date,
            total_permanent_hours,
            total_contract_hours,
            permanent_to_contract_ratio,
            permanent_staff_percentage,
            contract_staff_percentage
        FROM permanent_contract_staffing_ratio
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_state_metrics() -> pd.DataFrame:
    query = """
        SELECT
            state_nation,
            avg_short_stay_rehospitalization_rate_pct,
            avg_short_stay_op_emergency_dept_visit,
            avg_hospitalizations_per_1000_resident_days,
            avg_ed_visits_per_1000_resident_days
        FROM state_healthcare_metrics
        ORDER BY state_nation
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_hospitalizations() -> pd.DataFrame:
    query = """
        SELECT
            state_nation,
            avg_hospitalizations_per_1000
        FROM avg_number_hospitalizations
        ORDER BY avg_hospitalizations_per_1000 DESC
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_vaccination_comparison() -> pd.DataFrame:
    query = """
        SELECT
            state,
            resident_rate,
            staff_rate
        FROM resident_staff_vaccination_comparison
        ORDER BY state
    """

    return run_athena(query)


def convert_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:

    result = df.copy()

    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    return result


def convert_date_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:

    result = df.copy()

    for column in columns:
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], errors="coerce")

    return result


st.sidebar.header("Fliters")

page = st.sidebar.radio(
    "Select dashboard",
    [
        "Provider Capacity",
        "Staffing Analysis",
        "State Health Metrics",
        "Vaccination Analysis",
        "Summary"
    ]
)


# ---------------------------------------------------------
# Provider Capacity
# ---------------------------------------------------------

if page == "Provider Capacity":

    try:

        care_df = load_care_hours_per_bed()

        care_df = convert_numeric_columns(care_df, [
            "total_rn_hours",
            "total_lpn_hours",
            "total_cna_hours",
            "total_care_hours",
            "certified_beds",
            "total_care_hours_per_certified_bed"
        ])

        care_df = convert_date_columns(care_df, ["work_date"])

        provider_ids = sorted(
            care_df["provider_id"].dropna().unique().tolist())

        selected_provider = st.selectbox("Select provider", provider_ids)

        provider_df = care_df[care_df["provider_id"]
                              == selected_provider].copy()

        provider_df = provider_df.sort_values("work_date", ascending=True)

        latest_record = (provider_df.iloc[-1]
                         if not provider_df.empty else None)

        if latest_record is not None:
            col1, col2, col3 = st.columns(3)

            col1.metric("Certified Beds",
                        f"{latest_record['certified_beds']:.0f}")

            col2.metric("Total Care Hours",
                        f"{latest_record['total_care_hours']:.2f}")

            col3.metric("Care Hours per Bed",
                        (f"{latest_record['total_care_hours_per_certified_bed']:.2f}"))

        st.subheader("Total Care Hours Per Certified Bed By Provider")

        chart = px.line(provider_df,
                        x="work_date",
                        y="total_care_hours_per_certified_bed",
                        markers=True,
                        labels={
                            "work_date": "Work Date",
                            "total_care_hours_per_certified_bed":
                            "Care Hours per Certified Bed"
                        }
                        )

        st.plotly_chart(
            chart,
            use_container_width=True
        )

        st.dataframe(
            provider_df,
            use_container_width=True,
            hide_index=True
        )

    except Exception as error:
        st.error(
            f"Unable to load provider capacity data: {error}"
        )


# ---------------------------------------------------------
# Staffing Analysis
# ---------------------------------------------------------

elif page == "Staffing Analysis":

    try:
        staffing_df = load_staffing_ratio()

        staffing_df = convert_numeric_columns(
            staffing_df,
            [
                "total_permanent_hours",
                "total_contract_hours",
                "permanent_to_contract_ratio",
                "permanent_staff_percentage",
                "contract_staff_percentage"
            ]
        )

        states = sorted(staffing_df["state"].dropna().unique().tolist())

        selected_states = st.multiselect(
            "Select states", states, default=states[:5])

        if selected_states:
            filtered_staffing_df = staffing_df[staffing_df["state"].isin(
                selected_states)]
        else:
            filtered_staffing_df = staffing_df

        staffing_summary_df = (
            filtered_staffing_df
            .groupby("state", as_index=False)
            .agg(permanent_staff=("permanent_staff_percentage", "mean"),
                 contract_staff=(
                     "contract_staff_percentage", "mean")
                 )
        )

        staffing_long_df = staffing_summary_df.melt(
            id_vars="state",
            value_vars=[
                "permanent_staff",
                "contract_staff"
            ],
            var_name="staff_type",
            value_name="percentage"
        )

        st.subheader("Permanent and Contract Staff Ratio")

        staffing_chart = px.bar(staffing_long_df,
                                x="state",
                                y="percentage",
                                color="staff_type",
                                barmode="group",
                                labels={
                                    "state": "State",
                                    "percentage": "Average Percentage",
                                    "staff_type": "Staff Type"
                                }
                                )

        st.plotly_chart(
            staffing_chart,
            use_container_width=True
        )

        st.dataframe(
            staffing_long_df,
            use_container_width=True,
            hide_index=True
        )

    except Exception as error:
        st.error(
            f"Unable to load staffing dashboard: {error}"
        )


# ---------------------------------------------------------
# State Health Metrics
# ---------------------------------------------------------

elif page == "State Health Metrics":

    try:
        nursing_df = load_nursing_hours_per_patient()

        nursing_df = convert_numeric_columns(
            nursing_df,
            [
                "total_rn_hours",
                "total_lpn_hours",
                "total_cna_hours",
                "total_patients",
                "total_nursing_hours",
                "nurse_to_patient_ratio"
            ]
        )

        nursing_summary_df = (
            nursing_df
            .groupby("state", as_index=False)
            .agg(nurse_to_patient_ratio=("nurse_to_patient_ratio", "mean"))
            .sort_values("nurse_to_patient_ratio", ascending=False)
        )

        st.subheader(
            "Nursing Hours per Patient by State"
        )

        nursing_chart = px.choropleth(
            nursing_summary_df,
            locations="state",
            locationmode="USA-states",
            color="nurse_to_patient_ratio",
            scope="usa",
            hover_name="state",
            labels={
                "state": "State",
                "nurse_to_patient_ratio": "Average Nursing Hours per Patient"
            }
        )

        st.plotly_chart(
            nursing_chart,
            use_container_width=True
        )

        st.dataframe(
            nursing_df,
            use_container_width=True,
            hide_index=True
        )

    except Exception as error:
        st.error(
            f"Unable to load state metrics: {error}"
        )

# ---------------------------------------------------------
# Vaccination Analysis
# ---------------------------------------------------------

elif page == "Vaccination Analysis":

    try:
        vaccination_df = load_vaccination_comparison()

        vaccination_df = convert_numeric_columns(
            vaccination_df,
            [
                "resident_rate",
                "staff_rate"
            ]
        )

        vaccination_long_df = (
            vaccination_df.melt(
                id_vars="state",
                value_vars=[
                    "resident_rate",
                    "staff_rate"
                ],
                var_name="population",
                value_name="vaccination_rate"
            )
        )

        st.subheader(
            "Populations Vaccination Rate Per State"
        )

        chart = px.bar(
            vaccination_long_df,
            x="state",
            y="vaccination_rate",
            color="population",
            barmode="group",
            labels={
                "state": "State",
                "vaccination_rate":
                    "Vaccination Rate",
                "population": "Population"
            }
        )

        st.plotly_chart(
            chart,
            use_container_width=True
        )

        st.dataframe(
            vaccination_df,
            use_container_width=True,
            hide_index=True
        )

    except Exception as error:
        st.error(
            f"Unable to load vaccination dashboard: {error}"
        )
# ---------------------------------------------------------
# Overall Summary
# ---------------------------------------------------------

elif page == "Summary":

    try:
        top_provider_df = load_top_providers()

        vaccination_df = load_vaccination_comparison()

        hospitalization_df = load_hospitalizations()

        care_df = load_care_hours_per_bed()

        top_provider_df = convert_numeric_columns(
            top_provider_df,
            ["avg_residents_per_day"]
        )

        vaccination_df = convert_numeric_columns(
            vaccination_df,
            [
                "resident_rate",
                "staff_rate"
            ]
        )

        hospitalization_df = convert_numeric_columns(
            hospitalization_df,
            [
                "avg_hospitalizations_per_1000"
            ]
        )

        care_df = convert_numeric_columns(
            care_df,
            [
                "total_care_hours",
                "certified_beds",
                "total_care_hours_per_certified_bed"
            ]
        )

        average_resident_vaccination = (
            vaccination_df["resident_rate"].mean()
        )

        average_staff_vaccination = (
            vaccination_df["staff_rate"].mean()
        )

        average_care_hours_per_bed = (
            care_df[
                "total_care_hours_per_certified_bed"
            ].mean()
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Average Resident Vaccination",
            (
                f"{average_resident_vaccination:.2f}%"
                if pd.notna(
                    average_resident_vaccination
                )
                else "N/A"
            )
        )

        col2.metric(
            "Average Staff Vaccination",
            (
                f"{average_staff_vaccination:.2f}%"
                if pd.notna(
                    average_staff_vaccination
                )
                else "N/A"
            )
        )

        col3.metric(
            "Average Care Hours per Bed",
            (
                f"{average_care_hours_per_bed:.2f}"
                if pd.notna(
                    average_care_hours_per_bed
                )
                else "N/A"
            )
        )
        st.subheader(
            "Top 10 Providers by Average Residents"
        )

        provider_chart = px.bar(
            top_provider_df,
            x="avg_residents_per_day",
            y="provider_name",
            orientation="h",
            hover_data=[
                "provider_id",
                "state"
            ],
            labels={
                "avg_residents_per_day":
                    "Average Residents per Day",
                "provider_name":
                    "Provider"
            }
        )

        provider_chart.update_layout(
            yaxis={
                "categoryorder": "total ascending"
            }
        )

        st.plotly_chart(
            provider_chart,
            use_container_width=True
        )

        st.dataframe(
            top_provider_df,
            use_container_width=True
        )

    except Exception as error:
        st.error(
            f"Unable to load executive dashboard: {error}"
        )
