import pandas as pd
import plotly.express as px
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures

def task_create_scatter_plot(
    depends_on=BLD_data / "Summaries" / "aggregated_summary_GFSISB.csv",
    produces=BLD_figures / "scatter_plot_gfsibs.html"
):
    """Create a scatter plot using aggregated summary data."""
    
    if not depends_on.exists():
        print(f"File not found: {depends_on}")
        return
    
    # Load the aggregated summary
    df = pd.read_csv(depends_on)
    
    # Rename column if necessary
    df.rename(columns={'Stocks, Transactions, and Other Flows Name': 'Classification Name'}, inplace=True)
    
    # Define numeric columns for scatter plots
    x_col, y_col = 'Sum of Legitimate Entries', 'Number of Covered Countries'
    
    # Generate scatter plot
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        hover_data=['Classification Name', 'Sector Name', 'Unit Name'],
        title=f"Scatter Plot of {x_col} vs {y_col}",
        labels={x_col: x_col, y_col: y_col}
    )
    
    # Save the plot
    fig.write_html(produces)
    print(f"Scatter plot saved to {produces}")
