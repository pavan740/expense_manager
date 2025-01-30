import pandas as pd
import streamlit as st

# Data
data_df = pd.DataFrame(
    {
        "sales": [200, 550, 1100, 80],  # Example data with a value exceeding max_value
    }
)

# Create a new column to indicate overflow
max_value = 1000
data_df["status"] = data_df["sales"].apply(
    lambda x: "Exceeded" if x > max_value else "Within Limit"
)

# Customize display for the sales column
st.data_editor(
    data_df,
    column_config={
        "sales": st.column_config.ProgressColumn(
            "Sales volume",
            help="The sales volume in USD",
            format="$%f",
            min_value=0,
            max_value=max_value,
        ),
        "status": st.column_config.TextColumn(
            "Status",
            help="Indicates whether the sales volume exceeds the max value",
        ),
    },
    hide_index=True,
)
