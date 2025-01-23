import pandas as pd
import plotly.express as px
from itertools import combinations
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures

def task_create_scatter_plots(
        depends_on=[
            BLD_data / "Summaries" / "GFSISB" / "aggregated_summary_GFSISB.csv",
            BLD_data / "Summaries" / "GFSFALCS" / "aggregated_summary_GFSFALCS.csv",
            BLD_data / "Summaries" / "GFSMAB" / "aggregated_summary_GFSMAB.csv",
            BLD_data / "Summaries" / "GFSSSUC" / "aggregated_summary_GFSSSUC.csv"
        ],
        produces=BLD_figures / "scatter_plot.png"
):
    """Task to create scatter plots using aggregated summaries."""
    # Folders to process
    folders = ["GFSISB", "GFSFALCS", "GFSMAB", "GFSSSUC"]

    # Initialize an empty list to store DataFrames
    data_frames = []

    # Loop through each folder and load summaries
    for folder, file_path in zip(folders, depends_on):
        if file_path.exists():
            # Load the aggregated summary
            df = pd.read_csv(file_path)

            # Add a folder identifier for coloring
            df['Folder'] = folder

            # Rename column if necessary
            if folder == "GFSISB":
                df.rename(columns={'Stocks, Transactions, and Other Flows Name': 'Classification Name'}, inplace=True)

            data_frames.append(df)
        else:
            print(f"Aggregated file not found in {folder}.")

    # Combine all DataFrames into one
    agg_summary = pd.concat(data_frames, ignore_index=True)

    # Define numeric columns for scatter plots
    numeric_columns = ['Sum of Legitimate Entries', 'Number of Covered Countries']

    # Generate scatter plot for the first combination of numeric columns
    x_col, y_col = numeric_columns[0], numeric_columns[1]
    fig = px.scatter(
        agg_summary,
        x=x_col,
        y=y_col,
        color='Folder',  # Color by folder
        hover_data=['Classification Name', 'Sector Name', 'Unit Name'],
        title=f"Scatter Plot of {x_col} vs {y_col}",
        labels={x_col: x_col, y_col: y_col}
    )

    # Save the plot as an HTML file
    fig.write_html(produces)
    print(f"Scatter plot saved to {produces}")
