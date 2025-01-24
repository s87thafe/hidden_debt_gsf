import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data

def task_merge_gfsmab(
        depends_on=BLD_data / ".dir_created",
        produces=BLD_data / "Parquet" / "GFSMAB" / "filtered_merged_gsfmab.parquet"
):
    """
    Filters and merges data from multiple CSV files based on keywords.

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

    # Loop through the years and process each dataset in chunks
    for year in years:
        base_path = SRC / "data" / "GFSMAB"
        file_dir = f"GFSMAB{year}"
        file_name = f"GFSMAB{year}_01-16-2025.csv"
        file_path = base_path / file_dir / file_name
        
        if file_path.exists():
            print(f"Processing file for {year}: {file_path}")
            
            # Process the file in chunks
            chunksize = 10_000
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                # Add a vintage column
                chunk['Vintage'] = year
                
                # Filter the chunk
                filtered_chunk = filter_and_combine(chunk, 'Classification Name', keywords)            
                # Append filtered chunk to the list
                filtered_data_chunks.append(filtered_chunk)
        else:
            print(f"File not found for {year}: {file_path}")

    # Combine all filtered data and save as Parquet
    if filtered_data_chunks:
        final_filtered_data = pd.concat(filtered_data_chunks, ignore_index=True)
        final_filtered_data.to_parquet(produces, index=False)
        print(f"Filtered data saved to {produces}")
    else:
        print("No data to save.")
