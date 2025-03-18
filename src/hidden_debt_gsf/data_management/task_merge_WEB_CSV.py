import pandas as pd
from hidden_debt_gsf.config import SRC, BLD_data

def task_merge_gfsibs(
        depends_on=BLD_data / ".dir_created",
        produces=BLD_data / "Merged" / "filtered_merged_gsfibs.csv"
):
    """
    Filters and merges data from multiple CSV files based on keywords.

    Args:
        produces (str): Path to the output CSV file.
    """
    keywords = ["debt", "liabilities", "borrowing"]
    years = [2014, 2015, 2016, 2017, 2019, 2020, 2024]

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

        # Drop duplicate rows based on all columns
        combined_rows = combined_rows.drop_duplicates()

        return combined_rows

    # Initialize an empty list to collect filtered data
    filtered_data_chunks = []

    # Loop through the years and process each CSV file
    for year in years:
        csv_path = SRC / "data" / "WEB_CSV" / f"GFSIBS{year}.csv"

        if csv_path.exists():
            print(f"Processing file for {year}: {csv_path}")

            vintage_data = pd.read_csv(csv_path)
            # Add a vintage column
            vintage_data['Vintage'] = year

            # Filter the vintage
            filtered_data = filter_and_combine(vintage_data, 'Stocks, Transactions, and Other Flows Name', keywords)
            # Append filtered chunk to the list
            filtered_data_chunks.append(filtered_data)
        else:
            print(f"CSV file not found for {year}: {csv_path}")

    # Combine all filtered data and save as CSV
    if filtered_data_chunks:
        final_filtered_data = pd.concat(filtered_data_chunks, ignore_index=True)
        final_filtered_data.to_csv(produces, index=False)
        print(f"Filtered data saved to {produces}")
    else:
        print("No data to save.")
