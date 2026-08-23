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
def load_provider_ids(
    provider_type: str
) -> pd.DataFrame:

    query = f"""
        SELECT DISTINCT
            provider_id,
            provider_name
        FROM permanent_contract_staffing_ratio
        WHERE provider_type = '{provider_type}'
        ORDER BY provider_name
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_provider_types() -> pd.DataFrame:

    query = """
        SELECT DISTINCT
            provider_type
        FROM permanent_contract_staffing_ratio
        WHERE provider_type IS NOT NULL
        ORDER BY provider_type
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_bed_utilization(provider_id: str) -> pd.DataFrame:

    query = f"""
        SELECT
            provider_id,
            provider_name,
            state,
            certified_beds,
            avg_residents_per_day,
            bed_utilization_rate
        FROM provider_bed_utilization
        WHERE provider_id = '{provider_id}'
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_care_hours_per_bed(provider_id: str) -> pd.DataFrame:
    query = f"""
        SELECT
            provider_id,
            provider_name,
            work_date,
            total_rn_hours,
            total_lpn_hours,
            total_cna_hours,
            total_care_hours,
            certified_beds,
            total_care_hours_per_certified_bed
        FROM total_care_hours_per_certified_bed
        WHERE provider_id = '{provider_id}'
        ORDER BY work_date
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_average_care_hours_per_bed() -> pd.DataFrame:

    query = """
        SELECT
            AVG(
                total_care_hours_per_certified_bed
            ) AS average_care_hours_per_bed
        FROM total_care_hours_per_certified_bed
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_nursing_hours_per_patient() -> pd.DataFrame:

    query = """
        SELECT
            state,
            AVG(
                nurse_to_patient_ratio
            ) AS nurse_to_patient_ratio
        FROM avg_nurse_hours_to_patient_hospital
        GROUP BY state
        ORDER BY state
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_top_providers() -> pd.DataFrame:

    query = """
        SELECT
            provider_id,
            provider_name,
            state,
            certified_beds,
            avg_residents_per_day,
            bed_utilization_rate
        FROM provider_bed_utilization
        WHERE avg_residents_per_day IS NOT NULL
        ORDER BY avg_residents_per_day DESC
        LIMIT 10
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_staffing_states() -> pd.DataFrame:

    query = """
        SELECT DISTINCT
            state
        FROM permanent_contract_staffing_ratio
        ORDER BY state
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_staffing_ratio(
    selected_states: tuple[str, ...],
    provider_type: str
) -> pd.DataFrame:

    states_sql = ", ".join(
        f"'{state}'"
        for state in selected_states
    )

    query = f"""
        SELECT
            state,

            AVG(
                permanent_staff_percentage
            ) AS permanent_staff,

            AVG(
                contract_staff_percentage
            ) AS contract_staff

        FROM permanent_contract_staffing_ratio

        WHERE state IN ({states_sql})
          AND provider_type = '{provider_type}'

        GROUP BY state

        ORDER BY state
    """

    return run_athena(query)


@st.cache_data(ttl=300)
def load_overall_staffing_summary() -> pd.DataFrame:

    query = """
        SELECT
            AVG(
                permanent_staff_percentage
            ) AS avg_permanent_staff,

            AVG(
                contract_staff_percentage
            ) AS avg_contract_staff
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


st.sidebar.header("Filters")

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

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ---------------------------------------------------------
# Provider Capacity
# ---------------------------------------------------------

if page == "Provider Capacity":

    try:

        provider_types_df = load_provider_types()

        provider_types = (
            provider_types_df["provider_type"]
            .tolist()
        )

        selected_provider_type = st.selectbox(
            "Select provider type",
            provider_types
        )

        provider_df = load_provider_ids(
            selected_provider_type
        )

        provider_options = {
            f"{row['provider_name']} ({row['provider_id']})":
                row["provider_id"]
            for _, row in provider_df.iterrows()
        }

        selected_provider_label = st.selectbox(
            "Select provider",
            provider_options.keys()
        )

        selected_provider = (
            provider_options[selected_provider_label]
        )

        if selected_provider:

            care_df = load_care_hours_per_bed(
                selected_provider
            )

            care_df = convert_numeric_columns(
                care_df,
                [
                    "total_rn_hours",
                    "total_lpn_hours",
                    "total_cna_hours",
                    "total_care_hours",
                    "certified_beds",
                    "total_care_hours_per_certified_bed"
                ]
            )

            care_df = convert_date_columns(
                care_df,
                ["work_date"]
            )

            care_df = care_df.sort_values(
                "work_date",
                ascending=True
            )

            latest_record = (
                care_df.iloc[-1]
                if not care_df.empty
                else None
            )

            utilization_df = load_bed_utilization(
                selected_provider
            )

            utilization_df = convert_numeric_columns(
                utilization_df,
                [
                    "avg_residents_per_day",
                    "bed_utilization_rate"
                ]
            )

            utilization_record = (
                utilization_df.iloc[0]
                if not utilization_df.empty
                else None
            )

            if latest_record is not None:

                st.subheader(
                    latest_record["provider_name"]
                )

                st.caption(
                    f"Provider ID: {latest_record['provider_id']}"
                )

                col1, col2, col3, col4 = st.columns(4)

                col1.metric(
                    "Certified Beds",
                    f"{latest_record['certified_beds']:.0f}"
                )

                col2.metric(
                    "Total Care Hours",
                    f"{latest_record['total_care_hours']:.2f}"
                )

                col3.metric(
                    "Care Hours per Bed",
                    f"{latest_record[
                        'total_care_hours_per_certified_bed'
                    ]:.2f}"
                )

                col4.metric(
                    "Bed Utilization",
                    (
                        f"{utilization_record['bed_utilization_rate']:.2f}%"
                        if utilization_record is not None
                        and pd.notna(
                            utilization_record["bed_utilization_rate"]
                        )
                        else "N/A"
                    )
                )

            st.subheader(
                "Total Care Hours Per Certified Bed By Work Date"
            )

            chart = px.line(
                care_df,
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
                care_df,
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

        states_df = load_staffing_states()

        states = (
            states_df["state"]
            .tolist()
        )

        provider_types_df = load_provider_types()

        provider_types = (
            provider_types_df["provider_type"]
            .tolist()
        )

        selected_provider_type = st.selectbox(
            "Select provider type",
            provider_types,
            key="staffing_provider_type"
        )

        selected_states = st.multiselect(
            "Select states",
            states,
            default=states[:5]
        )

        if selected_states:

            staffing_summary_df = (
                load_staffing_ratio(
                    tuple(selected_states),
                    selected_provider_type
                )
            )

            staffing_summary_df = (
                convert_numeric_columns(
                    staffing_summary_df,
                    [
                        "permanent_staff",
                        "contract_staff"
                    ]
                )
            )

            staffing_long_df = (
                staffing_summary_df.melt(
                    id_vars="state",
                    value_vars=[
                        "permanent_staff",
                        "contract_staff"
                    ],
                    var_name="staff_type",
                    value_name="percentage"
                )
            )

            st.subheader(
                "Permanent and Contract Staff Ratio"
            )

            staffing_chart = px.bar(
                staffing_long_df,
                x="state",
                y="percentage",
                color="staff_type",
                barmode="group",
                labels={
                    "state": "State",
                    "percentage":
                        "Average Percentage",
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

        nursing_summary_df = load_nursing_hours_per_patient()

        state_metrics_df = load_state_metrics()

        nursing_summary_df = (
            convert_numeric_columns(
                nursing_summary_df,
                [
                    "nurse_to_patient_ratio"
                ]
            )
        )

        state_metrics_df = convert_numeric_columns(
            state_metrics_df,
            [
                "avg_short_stay_rehospitalization_rate_pct",
                "avg_short_stay_op_emergency_dept_visit",
                "avg_hospitalizations_per_1000_resident_days",
                "avg_ed_visits_per_1000_resident_days"
            ]
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
                "nurse_to_patient_ratio":
                    "Average Nursing Hours per Patient"
            }
        )

        st.plotly_chart(
            nursing_chart,
            use_container_width=True
        )

        st.subheader(
            "Average Rehospitalization Rate by State"
        )

        rehospitalization_chart = px.choropleth(
            state_metrics_df,
            locations="state_nation",
            locationmode="USA-states",
            color="avg_short_stay_rehospitalization_rate_pct",
            scope="usa",
            hover_name="state_nation",
            labels={
                "avg_short_stay_rehospitalization_rate_pct":
                    "Rehospitalization Rate (%)"
            }
        )

        st.plotly_chart(
            rehospitalization_chart,
            use_container_width=True
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
# Summary
# ---------------------------------------------------------

elif page == "Summary":

    try:
        top_provider_df = load_top_providers()

        average_care_df = load_average_care_hours_per_bed()

        nursing_summary_df = load_nursing_hours_per_patient()

        staffing_summary_df = load_overall_staffing_summary()

        vaccination_df = load_vaccination_comparison()

        top_provider_df = (
            top_provider_df
            .sort_values(
                "avg_residents_per_day",
                ascending=True
            )
        )

        average_care_df = convert_numeric_columns(
            average_care_df,
            [
                "average_care_hours_per_bed"
            ]
        )

        staffing_summary_df = convert_numeric_columns(
            staffing_summary_df,
            [
                "avg_permanent_staff",
                "avg_contract_staff"
            ]
        )

        nursing_summary_df = convert_numeric_columns(
            nursing_summary_df,
            ["nurse_to_patient_ratio"]
        )

        vaccination_df = convert_numeric_columns(
            vaccination_df,
            [
                "resident_rate",
                "staff_rate"
            ]
        )

        if not average_care_df.empty:

            average_care_hours_per_bed = (
                average_care_df[
                    "average_care_hours_per_bed"
                ].iloc[0]
            )

        else:
            average_care_hours_per_bed = None

        if not staffing_summary_df.empty:

            avg_permanent_staff = (
                staffing_summary_df[
                    "avg_permanent_staff"
                ].iloc[0]
            )

            avg_contract_staff = (
                staffing_summary_df[
                    "avg_contract_staff"
                ].iloc[0]
            )

        else:

            avg_permanent_staff = None
            avg_contract_staff = None

        average_resident_vaccination = (
            vaccination_df["resident_rate"].mean()
        )

        average_staff_vaccination = (
            vaccination_df["staff_rate"].mean()
        )

        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric(
            "Average Care Hours per Bed",
            (
                f"{average_care_hours_per_bed:.2f}"
                if pd.notna(
                    average_care_hours_per_bed
                )
                else "N/A"
            )
        )

        col2.metric(
            "Avg Permanent Staffing",
            (
                f"{avg_permanent_staff:.2f}%"
                if pd.notna(
                    avg_permanent_staff
                )
                else "N/A"
            )
        )

        col3.metric(
            "Avg Contract Staffing",
            (
                f"{avg_contract_staff:.2f}%"
                if pd.notna(
                    avg_contract_staff
                )
                else "N/A"
            )
        )

        col4.metric(
            "Average Resident Vaccination",
            (
                f"{average_resident_vaccination:.2f}%"
                if pd.notna(
                    average_resident_vaccination
                )
                else "N/A"
            )
        )

        col5.metric(
            "Average Staff Vaccination",
            (
                f"{average_staff_vaccination:.2f}%"
                if pd.notna(
                    average_staff_vaccination
                )
                else "N/A"
            )
        )

        provider_chart = px.bar(
            top_provider_df,
            x="avg_residents_per_day",
            y="provider_name",
            orientation="h",
            hover_data=[
                "provider_id",
                "state",
                "bed_utilization_rate"
            ],
            labels={
                "avg_residents_per_day":
                    "Average Residents per Day",
                "provider_name":
                    "Provider"
            }
        )

        staffing_mix_df = pd.DataFrame({
            "staff_type": [
                "Permanent Staff",
                "Contract Staff"
            ],
            "percentage": [
                avg_permanent_staff,
                avg_contract_staff
            ]
        })

        staffing_donut = px.pie(
            staffing_mix_df,
            names="staff_type",
            values="percentage",
            hole=0.45,
            title="Overall Staffing Mix"
        )

        left_col, right_col = st.columns(2)

        with left_col:
            st.subheader(
                "Top 10 Providers by Average Residents"
            )

            st.plotly_chart(
                provider_chart,
                use_container_width=True
            )

        with right_col:
            st.subheader(
                "Overall Staffing Mix"
            )

            st.plotly_chart(
                staffing_donut,
                use_container_width=True
            )

        st.subheader(
            "Average Nursing Hours per Patient by State"
        )

        state_map = px.choropleth(
            nursing_summary_df,
            locations="state",
            locationmode="USA-states",
            color="nurse_to_patient_ratio",
            scope="usa",
            hover_name="state",
            labels={
                "state": "State",
                "nurse_to_patient_ratio":
                    "Avg Nursing Hours per Patient"
            }
        )

        st.plotly_chart(
            state_map,
            use_container_width=True
        )

    except Exception as error:
        st.error(
            f"Unable to load executive dashboard: {error}"
        )
