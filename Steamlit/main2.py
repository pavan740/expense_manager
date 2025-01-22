import streamlit as st
import com_lib.common_methods as comm
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from st_aggrid import AgGrid, GridOptionsBuilder

st.set_page_config(layout="wide")


date_ranges = comm.get_month_date_ranges()
#print(date_ranges)
engine = comm.db_connection()

st.markdown("""
<style>
section[data-testid="stSidebar"] {
    width: 300px;  # Adjust the width as needed (e.g., 250px, 400px)
}
</style>
""", unsafe_allow_html=True)

st.sidebar.title("Date Selection")
show_data = st.sidebar.checkbox("Select Custom Date")
breakdown_start_date = ''
breakdown_end_date =''


if show_data:
    date_type = ['This Month', 'Last Month', 'Last Three Months', 'Last Six Months', 'Last Twelve Months', 'This Year', 'Last Year', 'All', 'Custom']
    filtered_date = st.sidebar.selectbox('Filter', options=date_type)
    if filtered_date == 'Custom':
        start_date = st.sidebar.date_input("Start Date")
        end_date = st.sidebar.date_input("End Date")
        if st.sidebar.button('Fetch'):
            breakdown_start_date = start_date
            breakdown_end_date = end_date
    elif filtered_date =='This Month':
        breakdown_start_date = date_ranges['current_month']['start_date']
        breakdown_end_date = date_ranges['current_month']['end_date']
    elif filtered_date =='Last Month':
        breakdown_start_date = date_ranges['last_month']['start_date']
        breakdown_end_date = date_ranges['last_month']['end_date']
    elif filtered_date == 'Last Three Months':
        breakdown_start_date = date_ranges['last_3_months']['start_date']
        breakdown_end_date = date_ranges['last_3_months']['end_date']
    elif filtered_date == 'Last Six Months':
        breakdown_start_date = date_ranges['last_6_months']['start_date']
        breakdown_end_date = date_ranges['last_6_months']['end_date']
    elif filtered_date == 'Last Twelve Months':
        breakdown_start_date = date_ranges['last_12_months']['start_date']
        breakdown_end_date = date_ranges['last_12_months']['end_date']
    elif filtered_date == 'This Year':
        breakdown_start_date = date_ranges['this_year']['start_date']
        breakdown_end_date = date_ranges['this_year']['end_date']
    elif filtered_date == 'Last Year':
        breakdown_start_date = date_ranges['last_year']['start_date']
        breakdown_end_date = date_ranges['last_year']['end_date']
    elif filtered_date == 'All':
        breakdown_start_date = date_ranges['All']['start_date']
        breakdown_end_date = date_ranges['All']['end_date']
else:
    breakdown_start_date = date_ranges['last_6_months']['start_date']
    breakdown_end_date = date_ranges['last_6_months']['end_date']



def query_maker(requirements):
    flex_variable= ''
    month_year = "strftime('%Y-%m', transaction_date) AS month_year,"
    group_by_month = "GROUP BY month_year"
    order_by_month_year = "ORDER BY month_year"

    if requirements == 'month_year':
        flex_variable = month_year
        flex_variable_end = f'{group_by_month}\n{order_by_month_year}'
    elif requirements == 'with_category':
        flex_variable = f"{month_year}Transaction_Type,"
        flex_variable_end = (f'{group_by_month},Transaction_Type\n{order_by_month_year}')
    elif requirements == 'all_data':
        flex_variable = '*,'
        flex_variable_end = 'GROUP BY id'
    else:
        flex_variable = ''
        flex_variable_end= ''

    query = f"""
        SELECT
        {flex_variable}
            SUM(CASE 
                    WHEN AmountSign = 'CR' THEN -Amount 
                    ELSE Amount 
                END) AS net_amount
        FROM 
            expenses_overview
        WHERE 
            Transaction_Date BETWEEN :start_date AND :end_date
        {flex_variable_end};
        """
    #print(query)
    return query

with engine.connect() as connection:
    current_month_result = connection.execute(
        text(query_maker('')),
        {"start_date": date_ranges['current_month']['start_date'],
         "end_date": date_ranges['current_month']['end_date']}
    ).fetchone()

current_net_amount = (current_month_result[0] if current_month_result else None) or 0.0

with engine.connect() as connection:
    last_month_result = connection.execute(
        text(query_maker('')),
        {"start_date": date_ranges['last_month']['start_date'],
         "end_date": date_ranges['last_month']['end_date']}
    ).fetchone()

last_month_net_amount = (last_month_result[0] if last_month_result else None) or 0.0

with engine.connect() as connection:
    lasttolast_month_result = connection.execute(
        text(query_maker('')),
        {#"start_date": '2021-01-31',
         "start_date": date_ranges['last_to_last_month']['start_date'],
         "end_date": date_ranges['last_to_last_month']['end_date']}
    ).fetchone()

lasttolast_month_net_amount = (lasttolast_month_result[0] if lasttolast_month_result else None) or 0.0

d1, d1str = comm.sign_based(last_month_net_amount-current_net_amount)
d2, d2str = comm.sign_based(lasttolast_month_net_amount -last_month_net_amount)

col1, col2 = st.columns(2)
col1.metric("This Month", f'{current_net_amount:,.0f}', f'{d1:,.0f} Rs. {d1str}', border=True)
col2.metric("Last Month", f'{last_month_net_amount:,.0f}', f'{d2:,.0f} Rs. {d2str}', border=True)

with st.container():
    month_breakdown_agg_df = pd.read_sql_query(query_maker('with_category'), engine,
                                               params={"start_date": date_ranges['current_month']['start_date'],
                                                       "end_date": date_ranges['current_month']['end_date']})
    if month_breakdown_agg_df.empty == False:
        st.markdown('Top Spending Categories in This Months')
        col1, col2, col3, col4, col5 = st.columns(5)
        category_totals = month_breakdown_agg_df.groupby('Transaction_Type')['net_amount'].sum().sort_values(
            ascending=False)
        category_totals = category_totals.sort_values(ascending=False)
        top_5_categories = category_totals.head(5)
        number_of_rows = top_5_categories.shape[0]
        cols = st.columns(number_of_rows)

        for i in range(number_of_rows):
            with cols[i]:
                st.metric(label=top_5_categories.index[i], value=f"{top_5_categories.values[i]:,.0f}", delta=None,
                          delta_color="normal", help=None, border=True)
with st.container():
    month_breakdown_agg_df = pd.read_sql_query(query_maker('with_category'), engine,
                                               params={"start_date": date_ranges['last_month']['start_date'],
                                                       "end_date": date_ranges['last_month']['end_date']})
    if month_breakdown_agg_df.empty == False:
        st.markdown('Top Spending Categories in Last Months')
        col1, col2, col3, col4, col5 = st.columns(5)
        category_totals = month_breakdown_agg_df.groupby('Transaction_Type')['net_amount'].sum().sort_values(
            ascending=False)
        category_totals = category_totals.sort_values(ascending=False)
        top_5_categories = category_totals.head(5)
        number_of_rows = top_5_categories.shape[0]
        cols = st.columns(number_of_rows)

        for i in range(number_of_rows):
            with cols[i]:
                st.metric(label=top_5_categories.index[i], value=f"{top_5_categories.values[i]:,.0f}", delta=None,
                          delta_color="normal", help=None, border=True)

with st.container():
    month_breakdown_agg_df = pd.read_sql_query(query_maker('with_category'), engine,
                                               params={"start_date": date_ranges['last_6_months']['start_date'],
                                                       "end_date": date_ranges['last_6_months']['end_date']})
    if month_breakdown_agg_df.empty == False:
        st.markdown('Top Spending Categories in last 6 Months')
        col1, col2, col3, col4, col5 = st.columns(5)
        category_totals = month_breakdown_agg_df.groupby('Transaction_Type')['net_amount'].sum().sort_values(
            ascending=False)
        category_totals = category_totals.sort_values(ascending=False)
        top_5_categories = category_totals.head(5)
        number_of_rows = top_5_categories.shape[0]
        cols = st.columns(number_of_rows)

        for i in range(number_of_rows):
            with cols[i]:
                st.metric(label=top_5_categories.index[i], value=f"{top_5_categories.values[i]:,.0f}", delta=None,
                          delta_color="normal", help=None, border=True)




with st.container():
    month_df = pd.read_sql_query(query_maker('month_year'), engine,
                                 params={"start_date": date_ranges['last_12_months']['start_date'],
                                         "end_date": date_ranges['last_12_months']['end_date']})
    month_df['month_year'] = pd.to_datetime(month_df['month_year'])
    fig = px.line(
        month_df,
        x='month_year',
        y='net_amount',
        title='Monthly Trend',
        labels={'month_year': 'Month-Year', 'net_amount': 'Total Expense'},
        markers=True  # Add markers to the line
    )
    fig.update_layout(showlegend=False, title_x=0.5, margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)

month_breakdown_df = pd.read_sql_query(query_maker('with_category'), engine,
                                           params={"start_date": breakdown_start_date,
                                                   "end_date": breakdown_end_date
                                                   })

with st.container():
    # Stacked Chart
    if month_breakdown_df.empty==False:
        months = month_breakdown_df['month_year'].unique().tolist()
        transaction_types = month_breakdown_df['Transaction_Type'].unique().tolist()
        net_amounts = []
        for month in months:
            for transaction_type in transaction_types:
                filtered_df = month_breakdown_df[(month_breakdown_df['month_year'] == month) &
                                                 (month_breakdown_df['Transaction_Type'] == transaction_type)]
                if not filtered_df.empty:
                    net_amounts.append(filtered_df['net_amount'].sum())
                else:
                    net_amounts.append(0)
        fig = go.Figure()

        for i, transaction_type in enumerate(transaction_types):
            fig.add_trace(go.Bar(
                x=months,
                y=net_amounts[i::len(transaction_types)],
                name=transaction_type
            ))

        fig.update_layout(
            title='Monthly Expense Breakdown',
            xaxis_title='Month-Year',
            yaxis_title='Net Amount',
            barmode='stack',
            title_x=0.5
        )
        st.plotly_chart(fig, use_container_width=True)


with st.container():
    new_monthly_pivot = pd.pivot_table(
        month_breakdown_df,
        values='net_amount',
        index='Transaction_Type',
        columns='month_year',
        fill_value=0,
        margins=True,
        aggfunc='sum',
        margins_name='ZTotal'
    )
    styled_df = new_monthly_pivot.style.background_gradient(cmap='Reds').format("{:.0f}")

    st.dataframe(styled_df, use_container_width=True
                 ,column_config={
                    'Transaction_Type': st.column_config.Column(width="medium")
                 })

with st.container():

    all_data_df = pd.read_sql_query(query_maker('all_data'), engine,
                                    params={"start_date": breakdown_start_date,
                                            "end_date": breakdown_end_date
                                            })
    if all_data_df.empty == False:
        columns = ['Transaction_Date', 'Transaction_Type', 'Description', 'Amount', 'AmountSign', 'Source_Name']
        all_data = all_data_df[columns]
        all_data = all_data.sort_values(by='Transaction_Date', ascending=False)
        gb = GridOptionsBuilder.from_dataframe(all_data)
        gb.configure_default_column(
            resizable=True,
            minWidth=100,
            flex=1,
            filter=True
        )
        gridOptions = gb.build()
        grid_response = AgGrid(
            all_data,
            gridOptions=gridOptions,
            height=400,
            fit_columns_on_grid_load=True
        )
