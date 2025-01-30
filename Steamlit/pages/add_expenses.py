import streamlit as st
import com_lib.common_methods as comm
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time

transaction_type, transaction_type_no_filter, source, not_required = comm.get_expense_categories()
notification_placeholder = st.empty()

# Title of the app
st.title("Add Expense")

#with st.form('my_form'):
    # 1. Date Selector
selected_date = st.date_input("Select a date")

# 2. Short Description Input
description = st.text_input("Write a short description")

# 3. Amount Input
amount = st.number_input("Enter amount")

# 4. First Dropdown Box
dropdown_option1 = st.selectbox("Select Transaction Type", options=transaction_type)

# 5. Second Dropdown Box
dropdown_option2 = st.selectbox("Select Source", options=source)

enable_textfield = st.checkbox("Is EMI?")
if enable_textfield:
    additional_text = st.number_input("Number of Months", min_value=0, step=1, format="%d")
else:
    additional_text = None

if st.button("Save"):
    # df = pd.DataFrame(columns=['Transaction_Date','Description','Amount','AmountSign','Type_ID','Source_ID','unkid','origin'])
    if amount < 0:
        AmountSign = 'CR'
    elif amount > 0:
        AmountSign = 'DR'

    df = pd.DataFrame(
        columns=['Transaction_Date', 'Description', 'Amount', 'Transaction_Type', 'Source_Name', 'AmountSign'])
    if additional_text not in [0, None]:
        for singlemonth in range(additional_text):
            new_date = selected_date + relativedelta(months=singlemonth)
            new_row = {
                'Transaction_Date': new_date,
                'Description': description,
                'Amount': amount,
                'Transaction_Type': dropdown_option1,
                'Source_Name': dropdown_option2,
                'AmountSign': AmountSign
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        new_row = {
            'Transaction_Date': selected_date,
            'Description': description,
            'Amount': amount,
            'Transaction_Type': dropdown_option1,
            'Source_Name': dropdown_option2,
            'AmountSign': AmountSign
        }
        df = pd.DataFrame([new_row])
    #st.dataframe(df)
    tran_added_source = 'Manual'
    df = comm.transaction_transform(df, tran_added_source)
    df['origin'] = tran_added_source
    engine = comm.db_connection()
    df.to_sql('expenses_transaction', con=engine, if_exists='append', index=False)
    notification_placeholder.success('Successfully Added.')
    time.sleep(3)
    notification_placeholder.empty()
    st.dataframe(df)
        #st.session_state["my_form"] = {}


