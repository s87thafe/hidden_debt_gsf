# This file contains the the proof, that no additional information can be gained from the Unit: Percentage of GDP. To do: automate with pytask


# Read the Parquet file
merged_df = pd.read_parquet('filtered_merged_gsfibs.parquet')

# Filter the DataFrame for Attribute == 'Value'
filtered_df = merged_df[merged_df['Attribute'] == 'Value']

# Define the grouping columns
grouped_columns = [
    'Country Name', 'Country Code', 'Stocks, Transactions, and Other Flows Name',
    'Stocks, Transactions, and Other Flows Code', 'Sector Name', 'Sector Code',
    'Residence Name', 'Residence Code', 'Instrument and Assets Classification Name',
    'Instrument and Assets Classification Code', 'Vintage'
]

# Split the data for Domestic Currency and Percent of GDP
domestic_currency = filtered_df[filtered_df['Unit Code'] == 'XDC']
percent_gdp = filtered_df[filtered_df['Unit Code'] == 'XDC_R_B1GQ']

# Aggregate data for each unit by grouping on relevant columns
domestic_presence = domestic_currency.groupby(grouped_columns)[list(map(str, range(1972, 2021)))].any()
percent_presence = percent_gdp.groupby(grouped_columns)[list(map(str, range(1972, 2021)))].any()

# Combine presence indicators to compare
combined_presence = percent_presence.join(domestic_presence, how='left', lsuffix='_percent', rsuffix='_domestic')

# Identify rows where Percent of GDP exists but Domestic Currency does not
missing_domestic = combined_presence[
    (combined_presence.filter(regex='_percent$').any(axis=1)) & 
    (~combined_presence.filter(regex='_domestic$').any(axis=1))
]

# Reset index for readability
missing_domestic = missing_domestic.reset_index()

# Extract rows from merged_df that match missing_domestic groups
missing_rows = filtered_df.merge(
    missing_domestic[grouped_columns],
    on=grouped_columns,
    how='inner'
)

# Save the result to a CSV file
missing_rows.to_csv('missing_domestic_entries.csv', index=False)

print("All related data entries have been saved to 'missing_domestic_entries.csv'.")