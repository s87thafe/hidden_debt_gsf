import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data

# The forbidden Task, as RAM is not sufficient to handle data size:

def convert_csv_to_parquet(
        depends_on=BLD_data / ".dir_created",
        produces=[BLD_data / "Parquet" / "GFSIBS" / f"GFSIBS_{year}.parquet" for year in [2014, 2015, 2016, 2017, 2019, 2020]]
):
    """
    Converts CSV files to Parquet format.
    """
    years = [2014, 2015, 2016, 2017, 2019, 2020]

    for year in years:
        base_path = SRC / "data" / "GFSIBS"
        file_dir = f"GFSIBS{year}"
        file_name = f"GFSIBS{year}_01-16-2025.csv"
        file_path = base_path / file_dir / file_name
        parquet_path = BLD_data / "Parquet" / "GFSIBS" / f"GFSIBS_{year}.parquet"

        if file_path.exists():
            print(f"Converting {file_path} to Parquet...")
            df = pd.read_csv(file_path)
            df.to_parquet(parquet_path, index=False)
            print(f"Saved to {parquet_path}")
            del df  # Explicitly delete the DataFrame to free up memory
        else:
            print(f"File not found for {year}: {file_path}")

def task_merge_gfsibs(
        depends_on=BLD_data / ".dir_created",
        produces=BLD_data / "Parquet" / "GFSIBS" / "filtered_merged_gsfibs.parquet"
):
    """
    Filters and merges data from multiple Parquet files based on keywords.

    Args:
        produces (str): Path to the output Parquet file.
    """
    keywords = ["debt", "liabilities", "borrowing"]
    years = [2014, 2015, 2016, 2017, 2019, 2020]

    def filter_and_combine(data, column_name, keywords):
        """
        Filter rows based on multiple keywords and return combined results
        without duplicates.
        """
        combined_rows = pd.DataFrame()
        for keyword in keywords:
            # Filter rows matching the keyword
            filtered_rows = data[data[column_name].str.contains(keyword, case=False, na=False)].copy()
            combined_rows = pd.concat([combined_rows, filtered_rows], ignore_index=True)

        # Drop duplicate rows based on all columns except the 'Keyword' column
        combined_rows = combined_rows.drop_duplicates(subset=data.columns.tolist())

        return combined_rows

    # Initialize an empty list to collect filtered data
    filtered_data_chunks = []

    # Loop through the years and process each Parquet file
    for year in years:
        parquet_path = SRC / "Parquet" / "GFSIBS" / f"GFSIBS_{year}.parquet"

        if parquet_path.exists():
            print(f"Processing file for {year}: {parquet_path}")

            vintage_data = pd.read_parquet(parquet_path)
            # Add a vintage column
            vintage_data['Vintage'] = year

            # Filter the vintage
            filtered_data = filter_and_combine(vintage_data, 'Stocks, Transactions, and Other Flows Name', keywords)
            # Append filtered chunk to the list
            filtered_data_chunks.append(filtered_data)
        else:
            print(f"Parquet file not found for {year}: {parquet_path}")

    # Combine all filtered data and save as Parquet
    if filtered_data_chunks:
        final_filtered_data = pd.concat(filtered_data_chunks, ignore_index=True)
        final_filtered_data.to_parquet(produces, index=False)
        print(f"Filtered data saved to {produces}")
    else:
        print("No data to save.")
