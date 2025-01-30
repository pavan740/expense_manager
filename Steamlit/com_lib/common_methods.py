from sqlalchemy import create_engine,select, MetaData, update
import hashlib
import pandas as pd
from datetime import datetime, timedelta, date
import os
def db_connection():
    db_path = os.path.join(os.getcwd(), 'database','expense_manager.db')
    engine = create_engine(f'sqlite:///{db_path}')
    return engine

def generate_id_transaction(row):
    combined_str = ''.join([str(row['Transaction_Date']), str(row['Description']), str(row['Amount']), str(row['AmountSign'])])
    unique_id = hashlib.md5(combined_str.encode()).hexdigest()
    return unique_id

def generate_id_for_raw_email(row):
    combined_str = ''.join([str(row['Time']), str(row['Snippet']), str(row['EmailSender'])])
    unique_id = hashlib.md5(combined_str.encode()).hexdigest()
    return unique_id


def format_date(date):
    date= str(date)
    if isinstance(date, str):
        #print('1')
        try:
            #print('2')
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                #print('3')
                # Format: "%Y-%m-%d %H:%M:%S"
                date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    #print('4')
                    # Format: "%d-%b-%Y" (e.g., "30-Dec-2023")
                    date = datetime.strptime(date, "%d-%b-%Y")
                except ValueError:
                    try:
                        date = datetime.strptime(date, "%Y-%m-%d")
                    except ValueError:
                        return None
                    # Return None if none of the formats match
            if isinstance(date, datetime):
                #print('6')
                return date.strftime("%Y-%m-%d")  # Return formatted date
    return None

def format_date_2(date):
    date = str(date)  # Ensure the input is a string
    if isinstance(date, str):
        # List of date formats to try
        date_formats = [
            "%Y-%m-%d %H:%M:%S.%f",  # Format: 2021-05-19 00:00:00.000000
            "%Y-%m-%d %H:%M:%S",     # Format: 2021-05-19 00:00:00
            "%d-%b-%Y",              # Format: 30-Dec-2023
            "%Y-%m-%d"               # Format: 2021-05-19
        ]

        # Try parsing the date with each format
        for fmt in date_formats:
            try:
                date = datetime.strptime(date, fmt)
                return date.strftime("%Y-%m-%d")  # Return in "YYYY-MM-DD" format
            except ValueError:
                continue  # Try the next format

    # Return None if none of the formats match
    return None


def transaction_transform(transactions_df, tran_source):
    engine = db_connection()
    expense_cat_df = pd.read_sql('SELECT * FROM expense_category', engine)
    source_df = pd.read_sql('SELECT * FROM sources', engine)
    expense_cat_df['Transaction_Type_lower'] = expense_cat_df['Transaction_Type'].str.lower()
    source_df['Source_Name_lower'] = source_df['Source_Name'].str.lower()
    transactions_df['Transaction_Type_lower'] = transactions_df['Transaction_Type'].str.lower()
    transactions_df['Source_Name_lower'] = transactions_df['Source_Name'].str.lower()
    transactions_df['Transaction_Date'] = transactions_df['Transaction_Date'].apply(format_date_2)
    transactions_df = pd.merge(transactions_df,
                               expense_cat_df[['Type_ID', 'Transaction_Type_lower', 'Transaction_Type']],
                               on='Transaction_Type_lower', how='left')
    transactions_df = pd.merge(transactions_df, source_df[['Source_ID', 'Source_Name_lower', 'Source_Name']],
                               on='Source_Name_lower', how='left')
    transactions_df = transactions_df.drop(
        ['Transaction_Type_y', 'Source_Name_y','Transaction_Type_x', 'Source_Name_x', 'Transaction_Type_lower', 'Source_Name_lower'], axis=1)
    if tran_source == 'Manual' or tran_source == 'Excel':
        transactions_df['unkid'] = transactions_df['Transaction_Date'].astype(str) + '_' + transactions_df['Description'].astype(str) + '_' + \
                                      transactions_df['Amount'].astype(str)
        transactions_df['unkid'] = transactions_df['unkid'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    if(tran_source!=''):
        transactions_df['origin'] = tran_source
    return transactions_df



def format_date_for_gmail(date):
    if isinstance(date, str):
        try:
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    if isinstance(date, datetime):
        return date.strftime("%Y/%m/%d") #+ "Z"
    return None



def fetch_sorted_unique_column(connection, table, column_name):
    query = select(table.c[column_name])
    result = connection.execute(query)
    return sorted(list(set([row[0] for row in result])))


def get_expense_categories():
    try:
        engine = db_connection()
        with engine.connect() as connection:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            expense_category_tb = metadata.tables['expense_category']
            sources_tb = metadata.tables['sources']

            # Fetch sorted unique columns
            """
            transaction_type = fetch_sorted_unique_column(connection, expense_category_tb, "Transaction_Type", Is_Active=1)
            sources = fetch_sorted_unique_column(connection, sources_tb, 'Source_Name')
            """

            transaction_type_query = (
                expense_category_tb.select()
                .with_only_columns(expense_category_tb.c.Transaction_Type)  # Positional argument
                .where(expense_category_tb.c.Is_Active == 1)
            )
            sources_query = (
                sources_tb.select()
                .with_only_columns(sources_tb.c.Source_Name)  # Positional argument
                .where(sources_tb.c.Is_Active == 1)
            )

            # Execute the queries and get unique sorted values
            transaction_type = sorted({row[0] for row in connection.execute(transaction_type_query)})
            sources = sorted({row[0] for row in connection.execute(sources_query)})
            sources_no_filter = fetch_sorted_unique_column(connection, sources_tb, 'Source_Name')
            transaction_type_no_filter = fetch_sorted_unique_column(connection, expense_category_tb, "Transaction_Type")

            return transaction_type,transaction_type_no_filter, sources, sources_no_filter

    except Exception as e:
        # Log the error or return a helpful message
        print(f"Error occurred: {e}")
        return None, None


def get_month_start_end():
    # Get the current date
    today = datetime.today()
    start_date = today.replace(day=1)

    next_month = today.replace(day=28) + timedelta(days=4)  # Move to next month
    end_date = next_month.replace(day=1) - timedelta(
        days=1)  # Subtract one day to get the last day of the current month

    return start_date, end_date


def get_month_date_ranges():
    today = date.today()
    current_year = today.year

    # Current Month
    current_month_start = today.replace(day=1)
    next_month_start = (current_month_start.replace(month=current_month_start.month + 1) if current_month_start.month < 12
                        else current_month_start.replace(year=current_month_start.year + 1, month=1))
    current_month_end = next_month_start - timedelta(days=1)
    this_month_end = next_month_start - timedelta(days=1)

    # Last Month
    last_month_end = current_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Month Before Last
    last_to_last_month_end = last_month_start - timedelta(days=1)
    last_to_last_month_start = last_to_last_month_end.replace(day=1)

    # Last 12 Months (starting from today)
    twelve_months_start = today - timedelta(days=365)
    twelve_months_end = today - timedelta(days=1)  # Ends the day before today

    # Last 6 Months (starting from today)
    six_months_start = (current_month_start - timedelta(days=182)).replace(day=1)
    six_months_end = this_month_end  # Ends the day before today

    three_months_start = (current_month_start - timedelta(days=90)).replace(day=1)
    three_months_end = this_month_end  # Ends the day before today

    this_year_start = today.replace(month=1, day=1)
    next_year_start = this_year_start.replace(year=this_year_start.year + 1)
    this_year_end = next_year_start - timedelta(days=1)

    this_year_start = today.replace(month=1, day=1)
    last_year_start = this_year_start.replace(year=current_year - 1)
    last_year_end = this_year_start - timedelta(days=1)

    # Since year 2000
    since_2000_start = '2000-01-01'
    since_2000_end = today

    return {
        "current_month": {"start_date": current_month_start, "end_date": current_month_end},
        "last_month": {"start_date": last_month_start, "end_date": last_month_end},
        "last_to_last_month": {"start_date": last_to_last_month_start, "end_date": last_to_last_month_end},
        "last_12_months": {"start_date": twelve_months_start, "end_date": twelve_months_end},
        "last_6_months": {"start_date": six_months_start, "end_date": six_months_end},
        "last_3_months": {"start_date": three_months_start, "end_date": three_months_end},
        "last_year": {"start_date": last_year_start, "end_date": last_year_end},
        "this_year": {"start_date": this_year_start, "end_date": this_year_end},
        "All": {"start_date": since_2000_start, "end_date": since_2000_end}
    }


def sign_based(input_delta):
    if input_delta < 0:
        delta_value = -abs(input_delta)
        delta_string = 'Exp. Increased'
    else:
        delta_value = abs(input_delta)
        delta_string = 'Exp. Decreased'
    delta_value = int(delta_value)
    return delta_value, delta_string