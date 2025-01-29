import streamlit as st
import pandas as pd
from sqlalchemy import text
import com_lib.common_methods as comm  # Assuming this contains your db_connection
import numpy as np

engine = comm.db_connection()
st.set_page_config(layout="wide")

st.title('Data Query')

if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame() # Initialize as empty DataFrame
if 'start_date' not in st.session_state:
    st.session_state.start_date = None
if 'end_date' not in st.session_state:
    st.session_state.end_date = None
if 'selected_source' not in st.session_state:
    st.session_state.selected_source = None
if 'edited_df' not in st.session_state:
    st.session_state.edited_df = None
modified_indices = pd.DataFrame()

transaction_type, transaction_type_no_filter, sources_with_filter, source = comm.get_expense_categories()
date_ranges = comm.get_month_date_ranges()
st.session_state.start_date = date_ranges['current_month']['start_date']
st.session_state.end_date = date_ranges['current_month']['end_date']


def clear_and_get_data(start_date,end_date,selected_source):
    #del st.session_state['df']
    query = text("""
           SELECT 
           Transaction_Date,
           Transaction_Type,
           Amount,
           AmountSign,
           Description,
           Source_Name,
           id
           FROM 
           expenses_overview 
           WHERE 
           Transaction_Date BETWEEN :start_date AND :end_date
           and (Source_Name = :source_name OR :source_name = 'All')
           ORDER BY 
           Transaction_Date desc;
       """)

    df = pd.read_sql(query, engine, params={
        "start_date": start_date,
        "end_date": end_date,
        "source_name": selected_source
    })

    df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date'])
    df['Delete'] = False

    return df


col1, col2, col3, col4,col5 = st.columns(5)
with col1:
    start_date = st.date_input("Start Date", value=st.session_state.start_date)
    st.session_state.start_date = start_date #update the session state
with col2:
    end_date = st.date_input("End Date", value=st.session_state.end_date)
    st.session_state.end_date = end_date #update the session state
with col3:
    source_new = np.insert(source, 0, 'All')
    selected_source = st.selectbox("Select Source", options=source_new, index=source_new.tolist().index(st.session_state.selected_source) if st.session_state.selected_source in source_new else 0)
    st.session_state.selected_source = selected_source #update the session state
with col4:
    if st.button('Get Data'):
        if "df" in st.session_state:
            del st.session_state.df
        st.session_state.df = clear_and_get_data(st.session_state.start_date,st.session_state.end_date,st.session_state.selected_source)



if 'original_df' not in st.session_state:
    st.session_state.original_df = st.session_state.df.copy()

if 'df' in st.session_state:
    df_shape = st.session_state.df.shape[0]
    if df_shape > 0:
        st.session_state.edited_df = st.data_editor(
            st.session_state.df,
            column_config={
                'Transaction_Date': st.column_config.DateColumn(
                    format="yyyy-MM-DD"
                ),
                'Transaction_Type': st.column_config.SelectboxColumn(
                    width='medium',
                    options=transaction_type_no_filter,
                    required=True,
                ),
                "Amount": st.column_config.NumberColumn(
                    step=0.1,  # Adjust step for decimal values
                    format="%d",
                ),
                'Source_Name': st.column_config.SelectboxColumn(
                    width='medium',
                    options=sources_with_filter,
                    required=True,
                )

            },
            disabled=['id'],
            hide_index=True,
            use_container_width=True
        )


if "edited_df" in st.session_state and "df" in st.session_state:
    if st.session_state.edited_df is not None and st.session_state.df is not None:
        modified_rows = st.session_state.edited_df != st.session_state.df
        modified_indices = modified_rows.any(axis=1)
        modified_records = st.session_state.edited_df[modified_indices]
        if not modified_records.empty:
            modified_records = modified_records.copy()
            modified_records['Transaction_Date'] = modified_records['Transaction_Date'].apply(comm.format_date_2)
            with st.container():
                st.dataframe(
                    modified_records,
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.write("No records have been modified.")
    else:
        st.write("Dataframes are not properly initialized.")
else:
    st.write("Session state does not contain the required dataframes.")


if st.button('Delete'):
    print('i am here')
    to_process = modified_rows

