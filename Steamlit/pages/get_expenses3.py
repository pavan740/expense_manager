import os
import base64
import time
from sqlalchemy import create_engine, Table, MetaData, update
from google_auth_oauthlib.flow import InstalledAppFlow
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import json
import openai
from bs4 import BeautifulSoup
import com_lib.common_methods as comm
from sqlalchemy import text
import re
import streamlit as st
import numpy as np
import hashlib
import pickle
from google.auth.transport.requests import Request
import configparser


st.title("Get Transactions from Email")
notification_placeholder = st.empty()


# If modifying the code later, delete the token.json to force re-authentication
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'import','config.ini'))
print(config['openai']['apikey'])
openai.api_key = config['openai']['apikey']

def llm_cleaning(email_body):
    response = openai.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
            {
                'role':'system',
                'content':[
                    {
                        "type": "text",
                        "text": "Source_ID\tSource_Name\tSource_Alias\n1\tICICI Amazon Pay\tXX4001\n2\tHDFC UPI CC\tXX5079\n3\tAxis Rewards\tXX8522\n4\tHDFC CC\tXX4882\n"
                                "The following are the sources. I will provide the email body, and the system should reply with a JSON output with the following details "
                                "and parameters: the Date should be in (YYYY-MM-DD) most of the time the date format given to you will be DD-MM-YYYY, Amount (Convert into INR), Source_Name, Description, "
                                "and AmountSign with the following condition: if it is an expense, it should be DR; if it is a refund, it should be CR."
                                "Always try to find Description of the transaction from the text."
                                "If the meaning of the message is failed transaction or doesn't have any transaction details then return blank. Dont show output with any information."
                                "Work with the only information provided to you. Dont makeup and details"
                    }
                ]
            },
            {
                'role':'user',
                'content': [
                    {
                        "type": "text",
                        "text": email_body
                    }
                ]
            }
        ],
    response_format = {"type": "json_object"},
    temperature = 0,
    max_completion_tokens = 3000,
    top_p = 1,
    frequency_penalty = 0,
    presence_penalty = 0
    )
    return response.choices[0].message.content

def authenticate():
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    credentials_file = os.path.join(os.getcwd(), 'import','credentials.json')
    credentials_file = credentials_file
    token_file = os.path.join(os.getcwd(), 'import','token.pickle')
    creds = None
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service


def list_messages(service, label_ids=['INBOX'], query=None):
    results = service.users().messages().list(userId='me', labelIds=label_ids, q=query).execute()
    messages = results.get('messages', [])
    #if not messages:
    #    print("No messages found.")
    #else:
    #    print(f"Found {len(messages)} message(s).")
    return messages

def get_message(service, msg_id):
    message = service.users().messages().get(userId='me', id=msg_id).execute()
    return message


def decode_body(body):
    try:
        return base64.urlsafe_b64decode(body).decode('utf-8')
    except Exception as e:
        #print(f"Error decoding body: {e}")
        return ''


def get_email_body(payload):
    if 'body' in payload and 'data' in payload['body']:
        return decode_body(payload['body']['data'])

    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain' and 'body' in part:
                return decode_body(part['body']['data'])
            elif part['mimeType'] == 'text/html' and 'body' in part:
                return decode_body(part['body']['data'])

    return ''


def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    for script_or_style in soup(['script', 'style', 'head', 'meta', 'footer']):
        script_or_style.decompose()
    text = soup.get_text(separator='\n', strip=True)

    return text

def get_emails(service, start_date, end_date):
    """Fetch emails between a specific date range."""
    query = f"after:{start_date} before:{end_date}"
    df = pd.DataFrame(columns=['EmailSender', 'Body', 'Time'])
    messages = list_messages(service, query=query)
    for msg in messages:
        msg_details = get_message(service, msg['id'])
        payload = msg_details['payload']
        emailtime = msg_details['internalDate']
        headers = payload['headers']
        snippet = msg_details.get('snippet', '')
        from_email = None
        for header in headers:
            if header['name'] == 'From':
                from_email = header['value']
                break

        body = get_email_body(payload)
        body = clean_html(body)
        date_time_obj = datetime.fromtimestamp(int(emailtime) / 1000)
        new_row = pd.DataFrame({'EmailSender':[from_email],
                                'Body':[body],
                                'Time':[date_time_obj],
                                'Snippet':snippet
                                })
        df = pd.concat([df, new_row], ignore_index=True)
    return df


def extract_email(text):
    email_regex = r'<([^>]+)>|([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    matches = re.findall(email_regex, text)
    for match in matches:
        return match[0] if match[0] else match[1]
    return None


def get_transaction():
    with st.status('Downloading Emails', expanded=True) as status:
        query = text("""
                     SELECT MAX(time) FROM raw_email
                     """)
        with engine.connect() as connection:
            result = connection.execute(query)
            max_time = result.scalar()
        max_time = comm.format_date_for_gmail(max_time)
        current_datetime = comm.format_date_for_gmail(datetime.now())
        st.write(f'Searching emails between {max_time} and {current_datetime}')
        service = authenticate()
        all_email = get_emails(service, max_time, current_datetime)
        old_df = pd.read_sql('select transaction_id from raw_email', engine)
        all_email['transaction_id'] = all_email['Time'].astype(str) + '_' + all_email['Snippet'].astype(str) + '_' + all_email['EmailSender'].astype(str)
        all_email['transaction_id'] = all_email['transaction_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        all_email = all_email[~all_email['transaction_id'].isin(old_df['transaction_id'])]
        df_shape = all_email.shape[0]

        if df_shape > 0:
            st.write(f'Found {df_shape} Emails')
            all_email['email'] = all_email['EmailSender'].apply(extract_email)
            all_email['llm_response'] = ''
            st.write(f'Waiting for OpenAI')
            for index, row in all_email.iterrows():
                all_email.loc[index, 'llm_response'] = llm_cleaning(row['Snippet'])
            st.write(f'Response Received from OpenAI')
            all_email.to_sql('raw_email', con=engine, if_exists='append', index=False)
            llm_response_df = pd.DataFrame()
            for index, row in all_email.iterrows():
                str_time = row['Time']
                tran_id = row['transaction_id']
                data = json.loads(row['llm_response'])
                if data:
                    df = pd.DataFrame([data])
                    df['Time'] = str_time
                    df['transaction_id'] = tran_id
                    llm_response_df = pd.concat([llm_response_df, df])
            try:
                llm_response_df['is_reviewed'] = 'False'
                llm_response_df['Discard'] = 'False'
                llm_response_df.to_sql('raw_response', con=engine, if_exists='append', index=False)
                st.write(f'{df_shape} Transaction(s) Saved in database.')
            except Exception as ex:
                st.write(f'Error Occurred\n{ex}')
        else:
            st.write(f'No New Messages Found!!')
    status.update(
        label="Download complete!", state="complete", expanded=False
    )

def save_transaction(df):
    result=df.query('Discard=="False"')
    result = result[['Date','Amount','Source_Name','Description','AmountSign','Transaction_Type','transaction_id']]
    result = result.rename(columns={'Date':'Transaction_Date',
                                    'transaction_id':'unkid'
                                    })
    expense_trans = pd.read_sql('select unkid from expenses_transaction', engine)
    new_result = comm.transaction_transform(result,'Email')
    new_result = new_result[~new_result['unkid'].isin(expense_trans['unkid'])]
    new_result.to_sql('expenses_transaction', con=engine, if_exists='append', index=False)
    ProcessStatus = 'Processed'
    is_reviewed = 'True'
    metadata = MetaData()
    metadata.bind = engine
    raw_response_table = Table('raw_response', metadata, autoload_with=engine)
    for index, row in df.iterrows():
        with engine.connect() as connection:
            stmt = update(raw_response_table). \
                where(raw_response_table.c.id == row['id']). \
                values(
                ProcessStatus=ProcessStatus,
                Discard=row['Discard'],
                is_reviewed=is_reviewed
            )
            result = connection.execute(stmt)
            connection.commit()

    notification_placeholder.success(f'Expenses Saved')

    time.sleep(3)
    notification_placeholder.empty()

def get_pending_tran(filtered_source, start_date_l, end_date_l):
    query = """
                    SELECT * FROM raw_response 
                    WHERE is_reviewed = 'False' AND (Source_Name = :source_name OR :source_name = 'All')
                    and Date BETWEEN :from_date AND :to_date
                    ORDER BY Date DESC
                """
    params = {'source_name': filtered_source, "from_date": start_date_l, "to_date": end_date_l}
    pending_transaction = pd.read_sql(query, engine, params=params)
    pending_transaction['Transaction_Type'] = pending_transaction['Source_Name'].apply(get_default_transaction_type)
    return pending_transaction

def get_default_transaction_type(source_name):
    if source_name == 'Axis Rewards':
        return 'Food'
    else:
        return 'Unknown'

engine = comm.db_connection()
pending_transaction = pd.DataFrame

with st.container():
    col1, col2, col3, col4 = st.columns(4, gap="small", vertical_alignment="center")
    start_date_l, end_date_l = comm.get_month_start_end()
    with col1:
        if st.button('Get Transactions'):
            get_transaction()
            #with st.spinner():

    with col2:
        start_date = st.date_input("Start Date", value=start_date_l)
    with col3:
        end_date = st.date_input("End Date", value=end_date_l)
    with col4:
        transaction_type, transaction_type_no_filter,source_with_filter,source = comm.get_expense_categories()
        source = np.insert(source, 0, 'All')
        filtered_source = st.selectbox('Filter Source', options=source)

pending_transaction = get_pending_tran(filtered_source, start_date, end_date)

#with col3:
with st.container():
    edited_df = st.data_editor(
        pending_transaction,
        column_order=['Date', 'Description', 'Amount', 'AmoungSign', 'Source_Name', 'Discard', 'Transaction_Type'],
        column_config={
            'Transaction_Type': st.column_config.SelectboxColumn(
                width='medium',
                options=transaction_type,
                required=True,
            ),
            'Discard': st.column_config.CheckboxColumn(
                default=False
            )
        },
        disabled=['Date', 'Description', 'Amount', 'AmoungSign', 'Source_Name'],
        hide_index=True,
        use_container_width=True
    )
    pending_transaction = edited_df

if st.button('Save'):
        save_transaction(pending_transaction)