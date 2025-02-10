import pandas as pd
from pathlib import Path
import plotly.express as px
from hidden_debt_gsf.config import SRC, BLD_data


def normalize_text(text):
    if pd.isna(text):
        return text
    text = text.lower().strip()
    if text.endswith('s'):
        text = text[:-1]  # Remove trailing 's'
    return text

def prepare_long_format(df, id_vars, value_vars, var_name, value_name):
    return df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name
    )

def read_dta(data_path, year):
    filename = f"gfs_{year}_CDROM.dta"
    file = data_path / "CD_DTA" / filename
    return pd.read_stata(file)

def process_dta_data(df_dta, year, debt_type="total"):
    suffix_mapping = {
        "total": "GG_33",
        "domestic": "GG_331",
        "foreign": "GG_332"
    }
    if debt_type not in suffix_mapping:
        raise ValueError("Invalid debt_type. Choose from 'total', 'domestic', or 'foreign'.")
    
    suffix = suffix_mapping[debt_type]
    filtered_df = df_dta[df_dta['TimeSeriesKey'].str.endswith(suffix, na=False)].copy()
    filtered_df['Vintage'] = int(year)
    filtered_df['Country Code'] = filtered_df['CTRY_CODE']
    filtered_df['Year'] = filtered_df['OStartYY']
    filtered_df['Value'] = filtered_df['OValue17']
    filtered_df['Rep_Basis'] = filtered_df.get('Rep_Basis', '')
    filtered_df['Sector Name'] = filtered_df.get('S_Desc', '')
    
    # Drop rows where 'Value' is nan
    filtered_df = filtered_df.dropna(subset=['Value'])
    
    # Drop rows where 'TimeSeriesKey' contains '_aZ_' or '_cZ_'
    pattern_drop = r'_aZ_|_cZ_'
    filtered_df = filtered_df[~filtered_df['TimeSeriesKey'].astype(str).str.contains(pattern_drop, regex=True, na=False)]
    
    # Handle duplicate entries by keeping only rows with '_aB_' or '_cB_' when needed
    duplicate_mask = filtered_df.duplicated(subset=['Year', 'Country Code', 'Vintage'], keep=False)
    duplicate_entries = filtered_df[duplicate_mask]
    pattern_keep = r'_aB_|_cB_'
    keep_entries = duplicate_entries[duplicate_entries['TimeSeriesKey'].astype(str).str.contains(pattern_keep, regex=True, na=False)]
    non_duplicate_entries = filtered_df[~duplicate_mask]
    final_df = pd.concat([non_duplicate_entries, keep_entries]).drop_duplicates()
    
    # Select required columns
    cols_to_keep = ['Country Code', 'Year', 'Vintage', 'Rep_Basis', 'Value', 'Sector Name', 'CTRY_NAME']
    return final_df[cols_to_keep]

def process_csv_data(df_csv, year, debt_type="total", start_year=1970, end_year=2025):
    residence_code_mapping = {
        "total": "W0|S1",
        "domestic": "W2|S1",
        "foreign": "W1|S1"
    }
    if debt_type not in residence_code_mapping:
        raise ValueError("Invalid debt_type. Choose from 'total', 'domestic', or 'foreign'.")
    
    filtered_df = df_csv[
        (df_csv['Unit Code'] == 'XDC') &
        (df_csv['Residence Code'] == residence_code_mapping[debt_type]) &
        (df_csv['Instrument and Assets Classification Code'] == 'F') &
        (df_csv['Stocks, Transactions, and Other Flows Code'] == 'G33') &
        (df_csv['Sector Code'] == 'S13')
    ].copy()
    
    sector_data = filtered_df.sort_values(by='Country Code').copy()
    sector_data['Vintage'] = int(year)
    
    year_columns = [col for col in sector_data.columns 
                    if col.isdigit() and start_year <= int(col) <= end_year]
    
    main_df = sector_data[sector_data["Attribute"] == "Value"]
    additional_info_df = sector_data[sector_data["Attribute"] == "Bases of recording (Cash/ Non Cash)"]
    
    df_long = prepare_long_format(
        main_df,
        id_vars=[c for c in sector_data.columns if c not in year_columns],
        value_vars=year_columns,
        var_name="Year",
        value_name="Value"
    ).dropna(subset=["Value"])
    
    additional_long = prepare_long_format(
        additional_info_df,
        id_vars=['Country Code'],
        value_vars=year_columns,
        var_name="Year",
        value_name="Cash_Accrual"
    )
    
    final_df = pd.merge(df_long, additional_long, on=['Country Code', 'Year'], how="left").fillna("")
    final_df.loc[final_df['Cash_Accrual'] == 'AC', 'Rep_Basis'] = 'Accrual'
    final_df.loc[final_df['Cash_Accrual'] == 'CA', 'Rep_Basis'] = 'Cash Basis'
    
    if 'Sector Name' not in final_df.columns:
        final_df['Sector Name'] = ''
    
    cols_to_keep = ['Country Code', 'Country Name', 'Year', 'Vintage', 'Rep_Basis', 'Value', 'Sector Name']
    final_df['Vintage'] = int(year)
    for col in cols_to_keep:
        if col not in final_df.columns:
            final_df[col] = ''
    return final_df[cols_to_keep]

def fill_missing_values(df, debt_type="total"):
    """
    Fill missing values in 'Country Name' by first forward/backward filling within each
    Country Code group, and then assigning the value from 'CTRY_NAME' if still missing.
    Finally, drop 'CTRY_NAME' and add extra columns.
    """
    # Define columns to fill
    columns_to_fill = ["Country Name"]
    
    # Sort by Country Code for consistency
    df = df.sort_values(by=["Country Code"]).copy()
    
    # Group by 'Country Code' and fill missing values using ffill and bfill
    for col in columns_to_fill:
        df[col] = df.groupby("Country Code")[col].transform(lambda group: group.ffill().bfill())
    
    # For any remaining NaN in 'Country Name', assign the value from 'CTRY_NAME'
    df["Country Name"].fillna(df["CTRY_NAME"], inplace=True)
    
    # Drop 'CTRY_NAME' as it is no longer needed
    df = df.drop(columns=["CTRY_NAME"])
    
    # Add additional columns
    df['Descriptor'] = 'Net incurrence of liabilities'
    df['Residence Name'] = debt_type
    
    return df

def filter_majority_basis(df):
    """
    For each country, keep only the reporting basis (Rep_Basis) that appears most frequently.
    """
    counts = df.groupby(['Country Code', 'Rep_Basis']).size().reset_index(name='count')
    chosen = counts.loc[counts.groupby('Country Code')['count'].idxmax(), ['Country Code', 'Rep_Basis']]
    filtered_df = pd.merge(df, chosen, on=['Country Code', 'Rep_Basis'], how='inner')
    return filtered_df

def calculate_vintage_diff(df):
    """
    For each Country Code and Year, calculate the difference in Value between each vintage 
    (ordered by the Vintage year) and its immediately preceding vintage.
    """
    # Ensure that Value and Vintage are numeric
    df["Value"] = pd.to_numeric(df["Value"], errors='coerce')
    df["Vintage"] = pd.to_numeric(df["Vintage"], errors='coerce')
    
    # Sort by Country Code, Year, and Vintage to ensure correct ordering
    df = df.sort_values(by=["Country Code", "Year", "Vintage"])
    
    # Compute the difference in Value with respect to the previous vintage within each group
    df["Value_Diff"] = df.groupby(["Country Code", "Year"])["Value"].diff()
    df["Value_Diff"] = df["Value_Diff"].fillna(0)  # Fill NaN values with 0
    # Calculate percentage change using the previous vintage's Value
    df["Value_Diff_Perc"] = df["Value_Diff"] / df.groupby(["Country Code", "Year"])["Value"].shift(1) * 100
    
    return df

def main_pipeline_filtered(data_path, dta_years, csv_years, debt_type="total"):
    processed_list = []
    
    # Process DTA files
    for year in dta_years:
        try:
            df_dta = read_dta(data_path, year)
            processed_df = process_dta_data(df_dta, year, debt_type)
            processed_list.append(processed_df)
        except Exception as e:
            print(f"Error processing DTA for year {year}: {e}")
    
    # Process CSV files
    for year in csv_years:
        try:
            folder = "WEB_CSV"
            csv_filename = f"GFSIBS{year}.csv"
            file_path = data_path / folder / csv_filename
            df_csv = pd.read_csv(file_path)
            processed_df = process_csv_data(df_csv, year, debt_type)
            processed_list.append(processed_df)
        except Exception as e:
            print(f"Error processing CSV for year {year}: {e}")
    
    # Combine all processed data
    combined_df = pd.concat(processed_list, ignore_index=True)
    
    # Ensure required columns are present
    cols_to_keep = ['Country Code', 'Country Name', 'Year', 'Vintage', 'Rep_Basis', 'Value', 'Sector Name', 'CTRY_NAME']
    combined_df = combined_df[cols_to_keep]
    
    # Convert data types and strip text fields
    combined_df['Country Code'] = combined_df['Country Code'].astype(int)
    combined_df['Year'] = combined_df['Year'].astype(int)
    combined_df['Vintage'] = combined_df['Vintage'].astype(int)
    combined_df['Rep_Basis'] = combined_df['Rep_Basis'].astype(str).str.strip()
    combined_df['Sector Name'] = combined_df['Sector Name'].astype(str).str.strip()

    # Filter out majority basis
    combined_df = filter_majority_basis(combined_df)
    
    # Apply fill_missing_values to fill missing 'Country Name'
    combined_df = fill_missing_values(combined_df, debt_type)

    combined_df = calculate_vintage_diff(combined_df)
    
    return combined_df

def clean_and_save_to_stata(df, output_path):
    """
    Cleans the given DataFrame to ensure compatibility with Stata and saves it as a .dta file.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to be cleaned and saved.
        output_path (str): The file path where the Stata file (.dta) should be saved.
    """
    # Replace infinite values with NaN
    df.replace([float('inf'), float('-inf')], pd.NA, inplace=True)

    # Convert object columns to string and fill None/NA with empty strings
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).fillna("")

    # Convert categorical-like columns to category type (optimization for Stata)
    categorical_columns = ["Country Name", "Rep_Basis", "Sector Name", "Descriptor", "Residence Name"]
    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Save the cleaned DataFrame to a Stata (.dta) file
    df.to_stata(output_path, write_index=False)
    print(f"Saved cleaned dataset to {output_path}")


produces_net_incurrence = {
    'dta' : BLD_data / "Merged" / "all_types_net_incurrence_liabilities.dta",
    'csv' : BLD_data / "Merged" / "all_types_net_incurrenence_liabilities.csv"
}

def task_merge_all_net_incurrence(
        depends_on=BLD_data / ".dir_created",
        produces = produces_net_incurrence
    ):
    # Define the data path and year lists.
    data_path = Path.cwd().resolve() / "src" / "hidden_debt_gsf" / "data"
    dta_years = ['2004', '2005', '2006', '2007', '2008', '2009', '2010', '2012', '2013']
    csv_years = ['2014', '2015', '2016', '2017', '2019', '2020', '2024']


    # Define the debt types to process.
    debt_types = ["total", "domestic", "foreign"]

    # List to hold DataFrames for each debt type.
    combined_list = []

    for dt in debt_types:
        # Run the pipeline for the given debt type.
        combined_data = main_pipeline_filtered(data_path, dta_years, csv_years, debt_type=dt)

        # Append the DataFrame for later concatenation.
        combined_list.append(combined_data)

    # Concatenate the datasets from all debt types.
    all_combined_data = pd.concat(combined_list, ignore_index=True)

    # Save the concatenated data as CSV.
    all_combined_data.to_csv(produces['csv'], index=False)
    clean_and_save_to_stata(df=all_combined_data, output_path=produces['dta'])
