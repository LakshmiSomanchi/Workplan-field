import streamlit as st
import pandas as pd

# Set page configuration for wider layout
st.set_page_config(layout="wide")

# --- Data Loading Function ---
@st.cache_data # Cache the data loading for faster re-runs if file content doesn't change
def load_data(uploaded_file, file_type):
    """
    Loads data from an uploaded file into a Pandas DataFrame.
    """
    if uploaded_file is not None:
        try:
            if file_type == "csv":
                df = pd.read_csv(uploaded_file)
            elif file_type == "excel":
                df = pd.read_excel(uploaded_file)
            else:
                st.error("Unsupported file type.")
                return None
            return df
        except Exception as e:
            st.error(f"Error loading file: {e}")
            return None
    return None

# --- KPI Calculation and Analysis Functions ---

def analyze_bmcs(bmc_df, farmer_df):
    """
    Analyzes BMC data against KPIs and identifies low-performing BMCs.
    Returns a dictionary of low-performing BMCs for each KPI.
    """
    if bmc_df is None:
        return {}

    # Ensure 'Date' column is datetime for filtering latest data
    if 'Date' in bmc_df.columns:
        bmc_df['Date'] = pd.to_datetime(bmc_df['Date'])
        # Get the latest data for each BMC
        latest_bmc_df = bmc_df.loc[bmc_df.groupby('BMC_ID')['Date'].idxmax()]
    else:
        latest_bmc_df = bmc_df.copy() # Use as is if no date column

    low_performing_bmcs = {
        'Quality': pd.DataFrame(),
        'Utilization': pd.DataFrame(),
        'Animal_Welfare': pd.DataFrame(),
        'Women_Empowerment': pd.DataFrame()
    }

    # --- KPI: Quality ---
    # Placeholder Thresholds - PLEASE ADJUST THESE VALUES BASED ON YOUR BUSINESS RULES
    QUALITY_FAT_THRESHOLD = 3.5
    QUALITY_SNF_THRESHOLD = 7.8

    low_quality_fat = latest_bmc_df[latest_bmc_df['Quality_Fat_Percentage'] < QUALITY_FAT_THRESHOLD]
    low_quality_snf = latest_bmc_df[latest_bmc_df['Quality_SNF_Percentage'] < QUALITY_SNF_THRESHOLD]
    adulteration_issues = latest_bmc_df[latest_bmc_df['Quality_Adulteration_Flag'].astype(str).str.lower() == 'yes']

    # Combine all quality issues
    low_performing_bmcs['Quality'] = pd.concat([low_quality_fat, low_quality_snf, adulteration_issues]).drop_duplicates(subset=['BMC_ID'])
    if not low_performing_bmcs['Quality'].empty:
        low_performing_bmcs['Quality']['Reason'] = 'Low Fat/SNF or Adulteration'


    # --- KPI: Utilization ---
    if 'Daily_Collection_Liters' in latest_bmc_df.columns and 'Capacity_Liters' in latest_bmc_df.columns:
        latest_bmc_df['Utilization_Percentage_Calculated'] = (latest_bmc_df['Daily_Collection_Liters'] / latest_bmc_df['Capacity_Liters']) * 100
        # Placeholder Threshold
        UTILIZATION_THRESHOLD = 70.0 # Below 70% is considered low
        low_performing_bmcs['Utilization'] = latest_bmc_df[latest_bmc_df['Utilization_Percentage_Calculated'] < UTILIZATION_THRESHOLD]
        if not low_performing_bmcs['Utilization'].empty:
            low_performing_bmcs['Utilization']['Reason'] = 'Low Utilization'

    # --- KPI: Animal Welfare Farms ---
    # Placeholder Threshold
    ANIMAL_WELFARE_THRESHOLD = 4.0 # Below 4.0 is considered low
    if 'Animal_Welfare_Compliance_Score_BMC' in latest_bmc_df.columns:
        low_performing_bmcs['Animal_Welfare'] = latest_bmc_df[latest_bmc_df['Animal_Welfare_Compliance_Score_BMC'] < ANIMAL_WELFARE_THRESHOLD]
        if not low_performing_bmcs['Animal_Welfare'].empty:
            low_performing_bmcs['Animal_Welfare']['Reason'] = 'Low Animal Welfare Score'

    # --- KPI: Women Empowerment ---
    # Placeholder Threshold
    WOMEN_EMPOWERMENT_THRESHOLD = 55.0 # Below 55% participation is considered low
    if 'Women_Empowerment_Participation_Rate_BMC' in latest_bmc_df.columns:
        low_performing_bmcs['Women_Empowerment'] = latest_bmc_df[latest_bmc_df['Women_Empowerment_Participation_Rate_BMC'] < WOMEN_EMPOWERMENT_THRESHOLD]
        if not low_performing_bmcs['Women_Empowerment'].empty:
            low_performing_bmcs['Women_Empowerment']['Reason'] = 'Low Women Empowerment Rate'

    return low_performing_bmcs


def generate_actionable_targets(low_bmcs_dict):
    """
    Generates actionable insights and suggested targets for low-performing BMCs.
    This is a simplified example. Real logic would be more complex.
    """
    action_items = []
    for kpi, df in low_bmcs_dict.items():
        if not df.empty:
            for index, row in df.iterrows():
                bmc_id = row['BMC_ID']
                district = row['District'] # Assuming District is always available

                if kpi == 'Quality':
                    current_fat = row.get('Quality_Fat_Percentage', 'N/A')
                    current_snf = row.get('Quality_SNF_Percentage', 'N/A')
                    adulteration = row.get('Quality_Adulteration_Flag', 'N/A')
                    action_items.append(
                        f"BMC {bmc_id} (District: {district}) has **Low Quality** (Fat: {current_fat}%, SNF: {current_snf}%, Adulteration: {adulteration}). "
                        f"**Action:** Field team to visit for quality checks, farmer awareness on clean milk production. "
                        f"**Target:** Increase Fat to >3.8% and SNF to >8.0% within 1 month."
                    )
                elif kpi == 'Utilization':
                    current_util = row.get('Utilization_Percentage_Calculated', 'N/A')
                    target_util = row.get('Utilization_Target_Percentage', '80') # Default for targets
                    action_items.append(
                        f"BMC {bmc_id} (District: {district}) has **Low Utilization** ({current_util:.2f}%). "
                        f"**Action:** Identify reasons for low collection, farmer mobilization, improve logistics. "
                        f"**Target:** Increase utilization to {target_util}% (or +5% points) within 2 months."
                    )
                elif kpi == 'Animal_Welfare':
                    current_score = row.get('Animal_Welfare_Compliance_Score_BMC', 'N/A')
                    action_items.append(
                        f"BMC {bmc_id} (District: {district}) has **Low Animal Welfare Score** ({current_score}). "
                        f"**Action:** Conduct farmer training on animal health, hygiene, and shelter. "
                        f"**Target:** Improve average animal welfare score to >4.5 within 3 months."
                    )
                elif kpi == 'Women_Empowerment':
                    current_rate = row.get('Women_Empowerment_Participation_Rate_BMC', 'N/A')
                    action_items.append(
                        f"BMC {bmc_id} (District: {district}) has **Low Women Empowerment Participation** ({current_rate:.2f}%). "
                        f"**Action:** Organize women's self-help group meetings, promote female farmer participation. "
                        f"**Target:** Increase women empowerment participation rate to >65% within 3 months."
                    )
    return action_items

# --- Streamlit App Layout ---

st.title("Ksheersagar Dairy Performance Dashboard")
st.markdown("---")

st.sidebar.header("Upload Data Files")

# File uploader for Farmer Data
farmer_file = st.sidebar.file_uploader("Upload Farmer Data (CSV/Excel)", type=["csv", "xlsx"], key="farmer_uploader")
farmer_df = load_data(farmer_file, "csv" if farmer_file and farmer_file.name.endswith('.csv') else "excel")

# File uploader for BMC Data
bmc_file = st.sidebar.file_uploader("Upload BMC Data (CSV/Excel)", type=["csv", "xlsx"], key="bmc_uploader")
bmc_df = load_data(bmc_file, "csv" if bmc_file and bmc_file.name.endswith('.csv') else "excel")

# File uploader for Field Team & Training Data
field_team_file = st.sidebar.file_uploader("Upload Field Team & Training Data (CSV/Excel)", type=["csv", "xlsx"], key="field_team_uploader")
field_team_df = load_data(field_team_file, "csv" if field_team_file and field_team_file.name.endswith('.csv') else "excel")

# Main content area
st.header("Data Overview & KPI Analysis")

if bmc_df is None:
    st.info("Please upload the BMC Data to begin the analysis.")
else:
    # Display loaded dataframes (optional, for verification)
    with st.expander("Show Raw Data Previews"):
        if farmer_df is not None:
            st.subheader("Farmer Data")
            st.dataframe(farmer_df.head())
        else:
            st.info("Farmer Data not uploaded.")

        if bmc_df is not None:
            st.subheader("BMC Data")
            st.dataframe(bmc_df.head())
        else:
            st.info("BMC Data not uploaded.") # Should not happen here if we are in this else block

        if field_team_df is not None:
            st.subheader("Field Team & Training Data")
            st.dataframe(field_team_df.head())
        else:
            st.info("Field Team & Training Data not uploaded.")

    st.markdown("---")
    st.header("KPI Performance Analysis")

    low_performing_bmcs = analyze_bmcs(bmc_df, farmer_df)

    if any(not df.empty for df in low_performing_bmcs.values()):
        st.subheader("Low Performing BMCs Identified:")
        for kpi, df in low_performing_bmcs.items():
            if not df.empty:
                st.write(f"#### {kpi.replace('_', ' ').title()} KPI Concerns:")
                st.dataframe(df[['BMC_ID', 'BMC_Name', 'District', 'Reason']].set_index('BMC_ID'))
                st.markdown("---")
    else:
        st.success("All BMCs are performing well across the defined KPIs based on current data!")

    st.header("Actionable Insights & Targets for Field Team")
    action_items = generate_actionable_targets(low_performing_bmcs)

    if action_items:
        for item in action_items:
            st.markdown(f"- {item}")
    else:
        st.info("No specific actionable insights or targets to display as all BMCs are performing well.")
