import streamlit as st
import pandas as pd
from io import StringIO
import os # To handle file paths and check for existence

# --- Configuration ---
# Define the directory where processed data will be stored
PROCESSED_DATA_DIR = "processed_data"
# Create the directory if it doesn't exist (important for Streamlit Cloud deployment)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

FARMERS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "farmers.parquet")
BMCS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "bmcs.parquet")
FIELD_TEAMS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "field_teams.parquet")

# --- Dummy Data Generation (as before, but now to be saved) ---
FARMERS_CSV_DATA = """
Farmer_ID,Farmer_Name,Village,District,BMC_ID,Milk_Production_Liters_Daily,Cattle_Count,Women_Empowerment_Flag,Animal_Welfare_Score
F001,Rajesh Kumar,Nandgaon,Pune,BMC001,15,5,No,4
F002,Priya Sharma,Lonikand,Pune,BMC002,22,8,Yes,5
F003,Amit Singh,Shirur,Pune,BMC001,18,6,No,3
F004,Sunita Devi,Daund,Pune,BMC003,12,4,Yes,4
F005,Mohan Rao,Bhor,Pune,BMC002,25,9,No,5
F006,Lakshmi Iyer,Junnar,Pune,BMC004,10,3,Yes,3
F007,Suresh Patil,Khed,Pune,BMC001,20,7,No,4
F008,Geeta Reddy,Mawal,Pune,BMC003,17,6,Yes,5
F009,Vikas Gupta,Mulshi,Pune,BMC002,14,5,No,3
F010,Anjali Mehta,Indapur,Pune,BMC004,28,10,Yes,4
F011,Deepak Kumar,Baramati,Pune,BMC005,10,3,No,3
F012,Ritu Singh,Ambegaon,Pune,BMC005,15,6,Yes,4
F013,Sandeep Gupta,Velhe,Pune,BMC006,12,4,No,2
F014,Pooja Sharma,Purandar,Pune,BMC006,18,7,Yes,3
"""

BMCS_CSV_DATA = """
BMC_ID,BMC_Name,District,Capacity_Liters,Daily_Collection_Liters,Quality_Fat_Percentage,Quality_SNF_Percentage,Quality_Adulteration_Flag,Quality_Target_Fat,Quality_Target_SNF,Utilization_Target_Percentage,Animal_Welfare_Compliance_Score_BMC,Women_Empowerment_Participation_Rate_BMC,Date
BMC001,Nandgaon BMC,Pune,1000,750,3.5,8.0,No,3.8,8.2,80,4.0,50,2025-07-15
BMC002,Lonikand BMC,Pune,1200,800,3.2,7.8,Yes,3.8,8.2,80,4.5,70,2025-07-15
BMC003,Daund BMC,Pune,800,700,3.9,8.1,No,3.8,8.2,80,4.2,60,2025-07-15
BMC004,Junnar BMC,Pune,900,600,3.6,7.9,No,3.8,8.2,80,3.8,40,2025-07-15
BMC005,Baramati BMC,Pune,1100,720,3.1,7.6,No,3.8,8.2,80,3.5,52,2025-07-15
BMC006,Velhe BMC,Pune,700,450,3.7,8.0,No,3.8,8.2,80,3.0,45,2025-07-15
BMC001,Nandgaon BMC,Pune,1000,780,3.6,8.1,No,3.8,8.2,80,4.1,55,2025-07-14
BMC002,Lonikand BMC,Pune,1200,850,3.3,7.9,No,3.8,8.2,80,4.6,75,2025-07-14
"""

FIELD_TEAMS_CSV_DATA = """
Team_ID,Team_Leader,District_Coverage,Max_BMC_Coverage,Training_Type,Training_Date,BMC_ID_Trained,Farmer_ID_Trained,Training_Outcome_Score
FT001,Ravi Kumar,Pune,5,Quality Improvement,2025-06-01,BMC001,,85
FT002,Sneha Singh,Pune,4,Animal Welfare,2025-05-15,BMC002,F002,90
FT001,Ravi Kumar,Pune,5,Women Empowerment,2025-06-20,,F004,88
FT003,Deepak Yadav,Pune,6,Utilization Efficiency,2025-07-01,BMC003,,80
FT004,Priya N.,Pune,4,Quality Improvement,2025-06-10,BMC005,,75
"""

# --- Data Management Function (simulating an offline process) ---
def generate_and_save_data_as_parquet():
    """
    Reads dummy CSV data, converts to DataFrames, and saves them as Parquet files.
    This function simulates an ETL process that you might run periodically.
    """
    farmer_df = pd.read_csv(StringIO(FARMERS_CSV_DATA))
    bmc_df = pd.read_csv(StringIO(BMCS_CSV_DATA))
    field_team_df = pd.read_csv(StringIO(FIELD_TEAMS_CSV_DATA))

    # Save to parquet
    farmer_df.to_parquet(FARMERS_PARQUET_PATH, index=False)
    bmc_df.to_parquet(BMCS_PARQUET_PATH, index=False)
    field_team_df.to_parquet(FIELD_TEAMS_PARQUET_PATH, index=False)

    st.success("Dummy data generated and saved as Parquet files!")

# --- Data Loading Function (from Parquet) ---
@st.cache_data(show_spinner="Loading data from processed files...") # Streamlit's built-in spinner
def load_processed_data():
    """
    Loads data from pre-processed Parquet files.
    This function is optimized for speed using cached Parquet reads.
    """
    try:
        farmer_df = pd.read_parquet(FARMERS_PARQUET_PATH)
        bmc_df = pd.read_parquet(BMCS_PARQUET_PATH)
        field_team_df = pd.read_parquet(FIELD_TEAMS_PARQUET_PATH)
        return farmer_df, bmc_df, field_team_df
    except FileNotFoundError:
        st.error("Processed data files not found. Please run the data generation script first.")
        return None, None, None
    except Exception as e:
        st.error(f"Error loading processed data: {e}")
        return None, None, None

# --- KPI Calculation and Analysis Functions (remain the same) ---

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

# Section to generate dummy processed data if it doesn't exist
st.sidebar.header("Data Management")
st.sidebar.info("For faster loading, data is read from pre-processed Parquet files.")
if not os.path.exists(FARMERS_PARQUET_PATH) or \
   not os.path.exists(BMCS_PARQUET_PATH) or \
   not os.path.exists(FIELD_TEAMS_PARQUET_PATH):
    st.sidebar.warning("Processed data files not found. Click to generate dummy data.")
    if st.sidebar.button("Generate Dummy Processed Data"):
        generate_and_save_data_as_parquet()
        st.experimental_rerun() # Rerun to load the newly generated files

# Load the processed data
farmer_df, bmc_df, field_team_df = load_processed_data()

if bmc_df is None:
    st.error("Could not load data. Please generate dummy data or check file paths.")
else:
    # Main content area
    st.header("Data Overview & KPI Analysis")

    # Display loaded dataframes (optional, for verification)
    with st.expander("Show Raw Data Previews"):
        st.subheader("Farmer Data")
        st.dataframe(farmer_df.head())

        st.subheader("BMC Data")
        st.dataframe(bmc_df.head())

        st.subheader("Field Team & Training Data")
        st.dataframe(field_team_df.head())

    st.markdown("---")
    st.header("KPI Performance Analysis")

    # Run analysis instantly
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
