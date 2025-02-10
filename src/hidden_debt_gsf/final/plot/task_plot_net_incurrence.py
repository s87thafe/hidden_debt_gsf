import pandas as pd
from pathlib import Path
import plotly.express as px
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures


def plot_vintage_diff_histogram(merged_vintage, debt_type="total", cap=100, output_folder= Path("hist")):
    """
    Create and display a histogram of percentage changes between consecutive vintages using Plotly.

    Parameters:
        merged_vintage (pd.DataFrame): DataFrame containing vintage differences.
            Must include the column 'Value_Diff_Perc' representing the percent change from the previous vintage.
        debt_type (str): Debt type used for labeling in the title.
        cap (int): Cap for outlier filtering on both sides.
    """
    # Drop NaNs from the percent change column
    percent_changes = merged_vintage['Value_Diff_Perc'].dropna()
    
    # Filter to keep values within [-cap, cap]
    percent_changes = percent_changes[(percent_changes >= -cap) & (percent_changes <= cap)]
    
    # Calculate the mean percentage change
    mean_percent_change = percent_changes.mean()

    min_val = 0.1

    # Filter out zero values and calculate non-zero prevalence
    percent_changes_nonzeros = percent_changes[percent_changes.abs() >= min_val]
    
    # Calculate counts to measure non-zero prevalence:
    total_non_nan = len(merged_vintage['Value_Diff_Perc'].dropna())
    non_zero_count = len(percent_changes_nonzeros)
    percentage_non_zero = round((non_zero_count / total_non_nan) * 100, 2) if total_non_nan > 0 else 0
    num_observations = non_zero_count

    # Create Plotly histogram with similar styling and title layout as your plot_percent_changes function
    fig = px.histogram(
        percent_changes_nonzeros,
        nbins=100,
        title=(
            f"Net incurrence of Liabilities {debt_type}:<br>"
            f"Percentual Changes between consecutive vintages<br>"
            f"Mean Percent Change: {mean_percent_change:.2f}% | Percentage non-zero: {percentage_non_zero}%<br>"
            f"Changing Observations: {num_observations}, zeros excluded (for all >=|{min_val}|), capped at |{cap}|"
        ),
        labels={'value': 'Percent Change (%)'},
        template='plotly_white'
    )
    fig.update_layout(
        xaxis_title="Percent Change (%)",
        yaxis_title="Frequency",
        title_x=0.5,
        xaxis_range=[-cap, cap]
    )
    output_path = output_folder / f"hist_net_incurrence_diff_{debt_type}.html"
    fig.write_html(output_path)

def plot_greece_net_incurrence_liabilities(combined_data, debt_type, output_folder=Path("greece")):
    """
    Filter the combined data for Greece and create a grouped bar chart for Value by Year and Vintage.
    The plot is saved as an HTML file in the specified output folder.
    
    Parameters:
        combined_data (pd.DataFrame): DataFrame containing the combined dataset for all debt types.
        debt_type (str): Debt type (e.g., 'total', 'domestic', or 'foreign') used for labeling.
        output_folder (str): Directory in which to save the HTML file.
    """
    # Ensure the output directory exists.
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # Filter for Greece (Country Code 174)
    greece_data = combined_data[combined_data['Country Code'] == 174].copy()
    
    # Convert Year and Vintage to string for proper categorical display.
    greece_data['Year'] = greece_data['Year'].astype(str)
    greece_data['Vintage'] = greece_data['Vintage'].astype(str)
    
    # Create a grouped bar chart with Year on the x-axis, Value on the y-axis, grouped by Vintage.
    fig = px.bar(
        greece_data,
        x="Year",
        y="Value",
        color="Vintage",
        barmode="group",
        title=f"Greece: Net Incurrence of Liabilities by Year and Vintage ({debt_type})"
    )
    
    # Customize the layout.
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Value",
        legend_title="Vintage"
    )
    
    # Build the output file path.
    output_file = output_folder / f"greece_{debt_type}_net_incurrence_liabilities.html"
    fig.write_html(str(output_file))
    fig.write_html(output_file)


def task_plot_net_incurrence(
        depends_on=BLD_data / "Merged" / "all_types_net_incurrenence_liabilities.csv"
):
    combined_data = pd.read_csv(depends_on)
    # Define the debt types to process.
    debt_types = ["total", "domestic", "foreign"]

    output_folder_greece=BLD_figures / "Greece" / "Net_Incurrence"
    output_folder_greece.mkdir(parents=True, exist_ok=True)
    output_folder_hist= BLD_figures / "Merged" / "Net_Incurrence"
    output_folder_hist.mkdir(parents=True, exist_ok=True)
    
    for dt in debt_types:
        #  Plot the debt stock for Greece.
        plot_greece_net_incurrence_liabilities(combined_data=combined_data, debt_type=dt, output_folder=output_folder_greece)
                
        # Plot the vintage differences histogram.
        plot_vintage_diff_histogram(merged_vintage=combined_data, debt_type=dt, cap=50, output_folder= output_folder_hist)
