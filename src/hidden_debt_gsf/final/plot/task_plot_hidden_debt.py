import pandas as pd
import plotly.express as px
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures

def task_top_country_sector_S13(
        depends_on=BLD_data / "DTA" / "GFSIBS" / "sector_datasets" / "sector_S13.csv",
        produces=BLD_figures / "Top_Country_S13_BarChart.html"
):
    """
    Processes the pivot data for Sector S13, identifies the country with the
    highest percentage change, and creates an interactive bar chart.

    Args:
        depends_on (Path): Path to the sector pivot CSV file.
        produces (Path): Path to save the generated bar chart HTML file.
    """
    # Load the pivot data for S13
    pivot_data = pd.read_csv(BLD_data / "DTA" / "GFSIBS" / "sector_datasets" / "sector_S13_pivot.csv")

    # Ensure 'Percent_Change' column exists and is numeric
    pivot_data['Percent_Change'] = pd.to_numeric(pivot_data['Percent_Change'], errors='coerce')

    # Step 1: Identify the country with the highest percentage change
    # Group by 'Country Code' and calculate the max percentage change
    country_max_change = (
        pivot_data.groupby('Country Code')['Percent_Change']
        .max()
        .reset_index(name='Max_Percent_Change')
        .sort_values(by='Max_Percent_Change', ascending=False)
    )

    # Get the country code with the 10th highest percentage change
    top_country_code = country_max_change.iloc[5]['Country Code']

    non_pivot = pd.read_csv(depends_on)

    # Step 2: Filter the data for the country with the highest percentage change
    filtered_data = non_pivot[non_pivot['Country Code'] == top_country_code]

    # Step 2: Identify year columns (1970 to 2020)
    year_columns = [col for col in filtered_data.columns if col.isdigit() and 1970 <= int(col) <= 2020]

    # Step 3: Reshape data to pivot by vintage
    pivot_data_new = (
        filtered_data.melt(
            id_vars=['Country Name', 'Sector Name', 'Vintage'], 
            value_vars=year_columns, 
            var_name='Year', 
            value_name='Value'
        )
    )

    # Ensure 'Value' is numeric
    pivot_data_new['Value'] = pd.to_numeric(pivot_data_new['Value'], errors='coerce')

    # Step 3: Reshape the data for grouped bar plot
    plot_data = pivot_data_new.dropna(subset=['Value'])  # Remove rows with NaN in 'Value'

    plot_data['Year'] = plot_data['Year'].astype(str)
    plot_data['Vintage'] = plot_data['Vintage'].astype(str)

    # Sort data for better alignment
    plot_data = plot_data.sort_values(by=['Year', 'Vintage'])

    country_name = pivot_data_new.iloc[0]['Country Name']

    # Step 4: Create the grouped bar chart
    fig = px.bar(
        plot_data,
        x='Year',
        y='Value',
        color='Vintage',
        title=(
            f"Yearly Values by Vintage for Country {country_name}, "
            f"Net incurrence of liabilities, General Government"
        ),
        labels={'Year': 'Year', 'Value': 'Value', 'Vintage': 'Vintage'},
        barmode='group'  # Group bars by Year and Vintage
    )

    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Value, in domestic currency',
        legend_title='Vintage',
        template='plotly_white',
        bargap=0.15,  # Adjust bar spacing for clarity
        bargroupgap=0.1,  # Adjust group spacing
        barmode='group'  # Ensures bars are grouped by Year and Vintage
    )

    # Save the chart as an HTML file
    fig.write_html(produces)

    print(f"Bar chart has been saved to {produces}.")
