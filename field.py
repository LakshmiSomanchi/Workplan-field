import streamlit as st
import pandas as pd

def load_data(uploaded_file):
    """
    Loads data from an uploaded CSV or Excel file into a Pandas DataFrame.
    """
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(uploaded_file)
            else:
                st.error("Unsupported file type. Please upload a CSV or Excel file.")
                return None
            return df
        except Exception as e:
            st.error(f"Error loading file: {e}")
            return None
    return None

st.set_page_config(layout="wide")
st.title("Ksheersagar Dairy Performance Dashboard")

st.sidebar.header("Upload Data Files")

# File uploader for Farmer Data
farmer_file = st.sidebar.file_uploader("Upload Farmer Data (CSV/Excel)", type=["csv", "xlsx"])
farmer_df = load_data(farmer_file)

# File uploader for BMC Data
bmc_file = st.sidebar.file_uploader("Upload BMC Data (CSV/Excel)", type=["csv", "xlsx"])
bmc_df = load_data(bmc_file)

# File uploader for Field Team & Training Data
field_team_file = st.sidebar.file_uploader("Upload Field Team & Training Data (CSV/Excel)", type=["csv", "xlsx"])
field_team_df = load_data(field_team_file)

# Display loaded dataframes (for verification)
st.header("Loaded Data Overview")

if farmer_df is not None:
    st.subheader("Farmer Data")
    st.dataframe(farmer_df.head())
else:
    st.info("Please upload Farmer Data to see the preview.")

if bmc_df is not None:
    st.subheader("BMC Data")
    st.dataframe(bmc_df.head())
else:
    st.info("Please upload BMC Data to see the preview.")

if field_team_df is not None:
    st.subheader("Field Team & Training Data")
    st.dataframe(field_team_df.head())
else:
    st.info("Please upload Field Team & Training Data to see the preview.")

st.markdown("---")
st.write("Once data is loaded, we'll proceed with analysis.")
