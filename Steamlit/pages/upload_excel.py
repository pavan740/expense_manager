import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import com_lib.common_methods as comm
import time

# Title of the app
st.title("Excel Sheet Selector")
notification_placeholder = st.empty()
# File uploader widget
uploaded_file = st.file_uploader("Upload an Excel file", type=["xls", "xlsx"])

if 'final_df' not in st.session_state:
    st.session_state.df = pd.DataFrame() # Initialize as empty DataFrame

def check_and_execute(df, required_columns):
  if set(required_columns).issubset(df.columns):
    try:
      return True
    except Exception as e:
      print(f"Error executing function: {e}")
      notification_placeholder.error(f'Error\n{e}')
      time.sleep(3)
      notification_placeholder.empty()
      return False
  else:
    missing_columns = set(required_columns) - set(df.columns)
    notification_placeholder.error(f"Missing columns: {missing_columns}")
    time.sleep(3)
    notification_placeholder.empty()
    return False

# Check if a file has been uploaded
if uploaded_file is not None:
    # Load the Excel file using pandas
    xls = pd.ExcelFile(uploaded_file)

    # Show the sheet names
    sheet_names = xls.sheet_names
    st.write("Sheets available in the Excel file:")
    #st.write(sheet_names)

    # Let the user select a sheet
    selected_sheet = st.selectbox("Select a sheet to load", options=sheet_names)

    # Load the selected sheet into a DataFrame
    if selected_sheet:

        st.write(f"Data from sheet: {selected_sheet}")
        with st.container():
            if st.button('Fetch Data'):
                df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
                st.dataframe(df , hide_index=True )
                columns_to_check = ['Transaction_Date','Description','Amount','AmountSign','Note','Transaction_Type','Source_Name']
                if check_and_execute(df, columns_to_check):
                    st.session_state.final_df = comm.transaction_transform(df, 'Excel')
                    gb = GridOptionsBuilder.from_dataframe(st.session_state.final_df)
                    gb.configure_default_column(
                        resizable=True,
                        minWidth=100,
                        flex=1,
                        filter=True
                    )
                    gridOptions = gb.build()
                    grid_response = AgGrid(
                        st.session_state.final_df,
                        gridOptions=gridOptions,
                        height=400,
                        fit_columns_on_grid_load=True
                    )



if st.button('Save File'):
    st.dataframe(st.session_state.final_df)
    #engine = comm.db_connection()
    #st.session_state.final_df.to_sql('expenses_transaction', con=engine, if_exists='append', index=False)
    notification_placeholder.success('Successfully Added.')
    time.sleep(3)
    notification_placeholder.empty()

