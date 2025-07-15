import pandas as pd
from io import StringIO
import os

# --- Configuration ---
PROCESSED_DATA_DIR = "processed_data"
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True) # Ensure the directory exists

FARMERS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "farmers.parquet")
BMCS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "bmcs.parquet")
FIELD_TEAMS_PARQUET_PATH = os.path.join(PROCESSED_DATA_DIR, "field_teams.parquet")

# --- Dummy Data (same as before) ---
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

# --- Data Generation and Saving Function ---
def generate_and_save_data_as_parquet():
    """
    Reads dummy CSV data, converts to DataFrames, and saves them as Parquet files.
    This function should be run as a standalone script or scheduled job.
    """
    print(f"--- Running data_manager.py at {pd.Timestamp.now()} ---")
    print(f"Ensuring directory {PROCESSED_DATA_DIR} exists...")
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    print("Loading data from embedded CSV strings...")
    farmer_df = pd.read_csv(StringIO(FARMERS_CSV_DATA))
    bmc_df = pd.read_csv(StringIO(BMCS_CSV_DATA))
    field_team_df = pd.read_csv(StringIO(FIELD_TEAMS_CSV_DATA))

    print("Saving data as Parquet files...")
    farmer_df.to_parquet(FARMERS_PARQUET_PATH, index=False)
    bmc_df.to_parquet(BMCS_PARQUET_PATH, index=False)
    field_team_df.to_parquet(FIELD_TEAMS_PARQUET_PATH, index=False)
    print("Data successfully generated and saved.")
    print("---------------------------------------")

if __name__ == "__main__":
    generate_and_save_data_as_parquet()
