import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data

def task_summarize_GFSISB(
        depends_on=BLD_data / ".dir_created",
        produces=BLD_data / "Summaries" / "aggregated_summary_GFSISB.csv"
):
    """Task to summarize GFSISB data."""
    # Define directories
    data_dir = SRC / "data" / "WEB_CSV"
    output_dir = BLD_data / "Summaries" / "GFSISB"
    output_dir.mkdir(parents=True, exist_ok=True)

    # List of years to process
    years = [2014, 2015, 2016, 2017, 2019, 2020, 2024]

    # List of keywords for filtering
    keywords = ["debt", "liabilities", "borrowing"]

    def load_data(data_dir, year):
        """Load the CSV file for a specific year in chunks."""
        file_name = f"GFSIBS{year}.csv"
        file_path = data_dir / file_name
        data = pd.read_csv(file_path)
        return data

    def filter_and_combine(data, column_name, keywords):
        """Filter rows based on multiple keywords and return combined results."""
        combined_rows = pd.DataFrame()
        for keyword in keywords:
            filtered_rows = data[data[column_name].str.contains(keyword, case=False, na=False)]
            filtered_rows['Keyword'] = keyword
            combined_rows = pd.concat([combined_rows, filtered_rows], ignore_index=True)
        return combined_rows

    def analyze_data_format(data):
        """Analyze data to calculate sums of legitimate entries and covered countries."""
        filtered_data = data[data['Attribute'] == 'Value']
        year_columns = [col for col in data.columns if col.startswith(('20', '19'))]
        grouped = filtered_data.groupby(
            ['Stocks, Transactions, and Other Flows Name', 'Sector Name', 'Unit Name', 
             'Residence Name', 'Instrument and Assets Classification Name']
        )
        summary = grouped.apply(
            lambda group: pd.Series({
                'Sum of Legitimate Entries': group[year_columns].apply(pd.to_numeric, errors='coerce').count().sum(),
                'Number of Covered Countries': group['Country Code'].nunique()
            })
        ).reset_index()
        return summary

    # Process all years
    summary_files = []
    for year in years:
        data = load_data(data_dir, year)
        if data is None:
            continue

        combined_data = filter_and_combine(data, "Stocks, Transactions, and Other Flows Name", keywords)
        summary = analyze_data_format(combined_data)

        summary_file = output_dir / f"summary_analysis_{year}.csv"
        summary.to_csv(summary_file, index=False)
        print(f"Summary for {year} saved to {summary_file}")
        summary_files.append(summary_file)

    # Aggregate all summaries
    summary_dfs = []
    for file_path in summary_files:
        if file_path.exists():
            summary_dfs.append(pd.read_csv(file_path))
        else:
            print(f"Summary file not found: {file_path}")

    combined_summary = pd.concat(summary_dfs, ignore_index=True)

    aggregated_summary = combined_summary.groupby(
        ['Stocks, Transactions, and Other Flows Name', 'Sector Name', 'Unit Name', 'Residence Name', 'Instrument and Assets Classification Name'],
        as_index=False
    ).agg({
        'Sum of Legitimate Entries': 'sum',
        'Number of Covered Countries': 'max'
    })

    aggregated_summary = aggregated_summary.sort_values(by='Sum of Legitimate Entries', ascending=False)
    aggregated_summary.to_csv(produces, index=False)
    print(f"Aggregated summary saved to {produces}")
