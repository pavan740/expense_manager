import streamlit as st
import pandas as pd
from sqlalchemy import text
import com_lib.common_methods as comm  # Assuming this contains your db_connection
import time

engine = comm.db_connection()
st.set_page_config(layout="wide")
st.title('Expense Type Management')
def get_expense_category():
    #del st.session_state['df']
    query = text("""
           SELECT 
           *
           from sources
           order by Source_Name;
       """)

    df = pd.read_sql(query, engine)
    df['Is_Active'] = df['Is_Active'].map({1: True, 0: False})
    return df


with st.container():
    df = get_expense_category()
    df_shape = df.shape[0]
    if df_shape > 0:
        edited_df = st.data_editor(
            df,
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic"
        )

if st.button('Save'):
    to_save = edited_df
    to_save['Is_Active'] = to_save['Is_Active'].map({True:1, False: 0})
    to_save.to_sql('sources', con=engine, if_exists='replace', index=False)
    placeholder = st.empty()
    placeholder.success('Source Updated!')
    time.sleep(2)
    placeholder.empty()



