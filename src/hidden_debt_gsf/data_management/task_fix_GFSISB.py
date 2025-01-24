import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures
import plotly.express as px

def task_most_populated_combinations_fix_gfsibs(
        depends_on=BLD_data / "Parquet" / "GFSIBS" / "filtered_merged_gsfibs.parquet",
        produces=BLD_data / "DTA" / "GFSIBS" / "most_populated_fix.dta"
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

    filtered_df = merged_df[
            (merged_df['Attribute'] == 'Value') & 
            (merged_df['Unit Code'] == 'XDC') & # Domestic Currency
            (merged_df['Residence Code'] == 'W0|S1') & # Total
            (merged_df['Instrument and Assets Classification Code'] == 'F') & # Total financial assets/liabilities
            (merged_df['Stocks, Transactions, and Other Flows Code'] == 'G33') # Net incurrence of liabilities
        ]

    # Step 3: Iterate over each country and process its data
    for country_code in unique_countries:
        # Filter data for the current country
        country_data = filtered_df[
            (merged_df['Country Code'] == country_code)
        ]

        # Skip if no data for the country
        if country_data.empty:
            continue

        # Identify year columns (1970 to 2020)
        year_columns = [col for col in country_data.columns if col.isdigit() and 1970 <= int(col) <= 2020]

        # Identify the most populated combination of Sector Code and Classification Code for the current country
        most_data_entries = (
            country_data
            .groupby(['Sector Code'])[year_columns]
            .apply(lambda group: group.notna().sum().sum())
            .reset_index(name='Data Entry Count')
            .sort_values(by='Data Entry Count', ascending=False)
        )

        # Get the most populated combination
        if not most_data_entries.empty:
            most_populated_combination = most_data_entries.iloc[0][['Sector Code']]
            
            # Filter data for the most populated combination
            most_populated_combination_data = country_data[
                (country_data['Sector Code'] == most_populated_combination['Sector Code'])
            ]
            
            # Append the result to the list
            most_populated_combinations.append(most_populated_combination_data)

    # Step 4: Combine all the most populated combination data into a single DataFrame
    combined_most_populated_combinations = pd.concat(most_populated_combinations, ignore_index=True)

    # Save the combined data to a .dta file
    combined_most_populated_combinations.to_stata(produces)

def task_all_sectors_fix_gfsibs(
        depends_on=BLD_data / "Parquet" / "GFSIBS" / "filtered_merged_gsfibs.parquet"
):
    """
    Processes the filtered data to generate separate datasets for each unique combination of Sector Code,
    calculates percentage changes, and creates interactive histograms for each sector.

    Args:
        depends_on (str): Path to the filtered data in parquet format.
    """
    import plotly.express as px

    # Step 1: Load the data
    merged_df = pd.read_parquet(depends_on)

    # Step 2: Filter the data for relevant conditions
    filtered_df = merged_df[
        (merged_df['Attribute'] == 'Value') &
        (merged_df['Unit Code'] == 'XDC') &  # Domestic Currency
        (merged_df['Residence Code'] == 'W0|S1') &  # Total
        (merged_df['Instrument and Assets Classification Code'] == 'F') &  # Total financial assets/liabilities
        (merged_df['Stocks, Transactions, and Other Flows Code'] == 'G33')  # Net incurrence of liabilities
    ]

    # Step 3: Create mapping of Sector Codes to Sector Names
    sector_mapping = {
        "S1311B": "Budgetary central government",
        "S1311": "Central government (excl. social security funds)",
        "S1321": "Central government (incl. social security funds)",
        "S13112": "Extrabudgetary central government",
        "S13": "General government",
        "S1313": "Local governments",
        "S1314": "Social security funds",
        "S1312": "State governments"
    }

    # Get unique Sector Codes
    unique_sector_codes = filtered_df['Sector Code'].unique()

    output_dir = BLD_data / "DTA" / "GFSIBS" / "sector_datasets"
    hist_dir = BLD_figures / "Hist_pivot"

    # Ensure output directories exist
    output_dir.mkdir(parents=True, exist_ok=True)
    hist_dir.mkdir(parents=True, exist_ok=True)

    # Step 4: Process data for each sector
    for sector_code in unique_sector_codes:
        # Filter data for the current Sector Code
        sector_data = filtered_df[filtered_df['Sector Code'] == sector_code]

        # Skip if no data for the sector
        if sector_data.empty:
            continue

        # Sort data by Country Code and Vintage
        sector_data = sector_data.sort_values(by=['Country Code', 'Vintage'])

        # Ensure all columns are of supported types
        for col in sector_data.columns:
            if sector_data[col].dtype == "object":
                if sector_data[col].isnull().all():
                    # Convert all-null object columns to float
                    sector_data[col] = sector_data[col].astype(float)
                elif sector_data[col].apply(lambda x: isinstance(x, str) or pd.isnull(x)).all():
                    # Convert object columns with strings or nulls to string
                    sector_data[col] = sector_data[col].astype(str)
                else:
                    raise ValueError(f"Unsupported column data type in '{col}' for Stata export.")


        # Create a file name for the dataset
        file_name = f"sector_{sector_code}.dta"

        # Save the dataset to the output directory
        sector_data.to_stata(output_dir / file_name)

        # Identify year columns
        year_columns = [col for col in sector_data.columns if col.isdigit() and 1970 <= int(col) <= 2020]

        # Reshape data for analysis
        pivot_data = (
            sector_data.melt(
                id_vars=['Country Code', 'Vintage'], 
                value_vars=year_columns, 
                var_name='Year', 
                value_name='Value'
            )
        )

        # Ensure 'Value' is numeric
        pivot_data['Value'] = pd.to_numeric(pivot_data['Value'], errors='coerce')

        # Calculate differences and percentage changes across vintages
        pivot_data['Year'] = pivot_data['Year'].astype(int)
        pivot_data['Vintage'] = pivot_data['Vintage'].astype(str)

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

        # Save the result to a CSV file
        file_name_pivot = f"sector_{sector_code}_pivot.csv"
        difference_data.to_csv(output_dir / file_name_pivot, index=False)

        # Step 7: Create and save interactive histograms for Percent_Change
        percent_changes = difference_data['Percent_Change'].dropna()
        percent_changes = percent_changes[(percent_changes != 0) & (percent_changes >= -50) & (percent_changes <= 50)]

        # Calculate the mean of percentage changes
        mean_percent_change = percent_changes.mean()
        num_observations = percent_changes.count()
        # Get the sector name for the title
        sector_name = sector_mapping.get(sector_code, "Unknown Sector")

        # Create Plotly histogram with the mean in the title
        fig = px.histogram(
            percent_changes, 
            nbins=500, 
            title=(
                f"Net incurrence of Liabilities: Histogram of Percentual Changes between Vintages for Sector {sector_name} ({sector_code})<br>"
                f"Mean Percent Change: {mean_percent_change:.2f}% | Observations: {num_observations}, no zeros, capped at |20|"
            ),
            labels={'value': 'Percent Change (%)'},
            template='plotly_white'
        )
        fig.update_layout(
            xaxis_title="Percent Change (%)",
            yaxis_title="Frequency",
            title_x=0.5,
            xaxis_range=[-20, 20]  # Restrict the x-axis range
        )

        # Save the histogram as an HTML file
        hist_file_name = f"sector_{sector_code}_histogram.html"
        fig.write_html(hist_dir / hist_file_name)

    print(f"Datasets and histograms have been saved to {output_dir} and {hist_dir}")



#    def task_calculate_vintage_differences_gfsibs(
#            depends_on=BLD_data / "DTA" / "GFSIBS" / "most_populated_float.csv",
#            produces=BLD_data / "DTA" / "GFSIBS" / "vintage_differences.csv"
#    ):
#        """
#        Processes the filtered data to calculate differences and percentage changes across vintages for all countries.
#
#        Args:
#            depends_on (str): Path to the input .csv file.
#            produces (str): Path to the output .csv file.
#        """
#        # Step 1: Load the data
#        most_populated_df = pd.read_csv(depends_on)
#
#        # Step 3: Identify year columns
#        year_columns = [col for col in most_populated_df.columns if col.isdigit() and 1970 <= int(col) <= 2020]
#
#        # Step 4: Reshape data for analysis
#        pivot_data = (
#            most_populated_df.melt(
#                id_vars=['Country Code', 'Vintage'], 
#                value_vars=year_columns, 
#                var_name='Year', 
#                value_name='Value'
#            )
#        )
#
#        # Ensure 'Value' is numeric
#        pivot_data['Value'] = pd.to_numeric(pivot_data['Value'], errors='coerce')
#
#        # Step 5: Calculate differences and percentage changes across vintages
#        pivot_data['Year'] = pivot_data['Year'].astype(int)
#        pivot_data['Vintage'] = pivot_data['Vintage'].astype(str)
#
#        # Group by country and year, and calculate differences and percentage changes
#        difference_data = (
#            pivot_data
#            .sort_values(by=['Country Code', 'Year', 'Vintage'])
#            .groupby(['Country Code', 'Year'], group_keys=False)
#            .apply(lambda group: group.assign(
#                Difference=group['Value'].diff(), 
#                Percent_Change=group['Value'].pct_change() * 100
#            ))
#            .dropna(subset=['Difference', 'Percent_Change'])  # Remove rows with NaN in either column
#        )
#
#        # Step 6: Save the result to a CSV file
#        difference_data.to_csv(produces, index=False)
#
#        print("Vintage differences and percentage changes have been calculated and saved to:", produces)