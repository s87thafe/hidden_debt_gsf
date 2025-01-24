import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data


def most_populated_combinations_gfsibs(
        depends_on=BLD_data / "Parquet" / "GFSIBS" / "filtered_merged_gsfibs.parquet",
        produces=BLD_data / "DTA" / "GFSIBS" / "most_populated_float.csv"
):
    """
    Processes the filtered data to identify and extract the most populated 
    combination of Sector Code and Classification Code for each country.

    Args:
        depends_on (dict): Dictionary containing dependencies, in this case, 
                           the path to the filtered data.
        produces (str): Path to the output .dta file.
    """
    merged_df = pd.read_parquet(depends_on)

    # Step 1: Initialize an empty list to store the most populated sector and classification data per country
    most_populated_combinations = []

    # Step 2: Get a list of unique country codes
    unique_countries = merged_df['Country Code'].unique()

    # Step 3: Iterate over each country and process its data
    for country_code in unique_countries:
        # Filter data for the current country
        country_data = merged_df[
            (merged_df['Country Code'] == country_code) & 
            (merged_df['Attribute'] == 'Value') & 
            (merged_df['Unit Code'] == 'XDC') & # Domestic Currency
            (merged_df['Residence Code'] == 'W0|S1') & # Total
            (merged_df['Instrument and Assets Classification Code'] == 'F') & # Total financial assets/liabilities
            (merged_df['Sector Code']!= 'S1312') & # State Governments
            (merged_df['Sector Code']!= 'S1313') & # Local Governments
            (merged_df['Sector Code']!= 'S1314') # Social security funds
        ]

        # Skip if no data for the country
        if country_data.empty:
            continue

        # Identify year columns (1970 to 2020)
        year_columns = [col for col in country_data.columns if col.isdigit() and 1970 <= int(col) <= 2020]

        # Identify the most populated combination of Sector Code and Classification Code for the current country
        most_data_entries = (
            country_data
            .groupby(['Sector Code', 'Stocks, Transactions, and Other Flows Code'])[year_columns]
            .apply(lambda group: group.notna().sum().sum())
            .reset_index(name='Data Entry Count')
            .sort_values(by='Data Entry Count', ascending=False)
        )

        # Get the most populated combination
        if not most_data_entries.empty:
            most_populated_combination = most_data_entries.iloc[0][['Sector Code', 'Stocks, Transactions, and Other Flows Code']]
            
            # Filter data for the most populated combination
            most_populated_combination_data = country_data[
                (country_data['Sector Code'] == most_populated_combination['Sector Code']) &
                (country_data['Stocks, Transactions, and Other Flows Code'] == most_populated_combination['Stocks, Transactions, and Other Flows Code'])
            ]
            
            # Append the result to the list
            most_populated_combinations.append(most_populated_combination_data)

    # Step 4: Combine all the most populated combination data into a single DataFrame
    combined_most_populated_combinations = pd.concat(most_populated_combinations, ignore_index=True)

    # Save the combined data to a .dta file
    combined_most_populated_combinations.to_csv(produces)


def task_calculate_vintage_differences_gfsibs(
        depends_on=BLD_data / "DTA" / "GFSIBS" / "most_populated_float.csv",
        produces=BLD_data / "DTA" / "GFSIBS" / "vintage_differences_float.csv"
):
    """
    Processes the filtered data to calculate differences and percentage changes across vintages for all countries.

    Args:
        depends_on (str): Path to the input .csv file.
        produces (str): Path to the output .csv file.
    """
    # Step 1: Load the data
    most_populated_df = pd.read_csv(depends_on)

    # Step 3: Identify year columns
    year_columns = [col for col in most_populated_df.columns if col.isdigit() and 1970 <= int(col) <= 2020]

    # Step 4: Reshape data for analysis
    pivot_data = (
        most_populated_df.melt(
            id_vars=['Country Code', 'Vintage'], 
            value_vars=year_columns, 
            var_name='Year', 
            value_name='Value'
        )
    )

    # Ensure 'Value' is numeric
    pivot_data['Value'] = pd.to_numeric(pivot_data['Value'], errors='coerce')

    # Step 5: Calculate differences and percentage changes across vintages
    pivot_data['Year'] = pivot_data['Year'].astype(int)
    pivot_data['Vintage'] = pivot_data['Vintage'].astype(str)

    # Group by country and year, and calculate differences and percentage changes
    difference_data = (
        pivot_data
        .sort_values(by=['Country Code', 'Year', 'Vintage'])
        .groupby(['Country Code', 'Year'], group_keys=False)
        .apply(lambda group: group.assign(
            Difference=group['Value'].diff(), 
            Percent_Change=group['Value'].pct_change() * 100
        ))
        .dropna(subset=['Difference', 'Percent_Change'])  # Remove rows with NaN in either column
    )

    # Step 6: Save the result to a CSV file
    difference_data.to_csv(produces, index=False)

    print("Vintage differences and percentage changes have been calculated and saved to:", produces)

