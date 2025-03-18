import pandas as pd
from hidden_debt_gsf.config import BLD_data

def task_generate_country_specific_files(
        depends_on=BLD_data / "Merged" / "all_types_net_incurrenence_liabilities.csv",
        produces=BLD_data / "Merged" / "net_incurrence_outlier_filtered.dta"
):
    # Read CSV file
    combined_data = pd.read_csv(depends_on)

    # Sort by Country Code, Year, and Vintage to ensure correct ordering
    combined_data = combined_data.sort_values(by=["Country Code", "Year", "Vintage", "Residence Name"])

    # Function to remove the first row where Value == 0.0 *only if* the next Vintage has a nonzero value
    def drop_initial_zero_entries(group):
        if len(group) > 1 and group["Value"].iloc[0] == 0.0 and group["Value"].iloc[1] != 0.0:
            return group.iloc[1:]  # Drop first row
        return group  # Keep all rows if condition is not met

    # Apply function to each group
    combined_data = combined_data.groupby(["Country Code", "Year", "Residence Name"], group_keys=False).apply(drop_initial_zero_entries)

    # Country specific adjustments:
    # Belarus: currency reform 2016 (1 new ruble = 10,000 old rubles)
    combined_data.loc[
        (combined_data["Country Code"] == 913) & (combined_data["Vintage"] < 2017),
        "Value"
    ] *= 1/10000 # Source: Web

    # Slovakia: currency change: reported in Koruny, switched then to Euro
    combined_data.loc[
        (combined_data["Country Code"] == 936) & (combined_data["Vintage"] <= 2012) & (combined_data["Year"] <= 2008),
        "Value"
    ] *= 1/30.12592 # Source: Difference in Data

    # Slovenia: currency change: reported in Tolars, switched then to Euro
    combined_data.loc[
        (combined_data["Country Code"] == 961) & (combined_data["Vintage"] <= 2012) & (combined_data["Year"] <= 2006),
        "Value"
    ] *= 1/239.6357537 # Source: Difference in Data

    # France: currency change: reported in Franc, switched then to Euro
    combined_data.loc[
        (combined_data["Country Code"] == 132) & (combined_data["Vintage"] <= 2012) & (combined_data["Year"] <= 1989),
        "Value"
    ] *= 1/6.559527714 # Source: Difference in Data

    # Malta: currency change: reported in Lira, switched then to Euro
    combined_data.loc[
        (combined_data["Country Code"] == 181) & (combined_data["Vintage"] <= 2012) & (combined_data["Year"] <= 2007),
        "Value"
    ] *= 3.03609922 # Source: Difference in Data

    # Estonia: currency change: reported in Kroon, switched then to Euro
    combined_data.loc[
        (combined_data["Country Code"] == 939) & (combined_data["Vintage"] < 2012) & (combined_data["Year"] <= 2008),
        "Value"
    ] *= 1/15.408140379155446 # Source: Difference in Data

    # Compute the difference in Value with respect to the previous vintage within each group
    combined_data["Value_Diff"] = combined_data.groupby(["Country Code", "Year", "Residence Name"])["Value"].diff()

    # Calculate percentage change using the previous vintage's Value
    combined_data["Value_Diff_Perc"] = combined_data["Value_Diff"] / combined_data.groupby(["Country Code", "Year", "Residence Name"])["Value"].shift(1) * 100

    # Calculate absolute percentual change for "Value_Diff_Perc"
    combined_data["Abs_Diff_Perc"] = combined_data["Value_Diff_Perc"].abs()

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

    clean_and_save_to_stata(combined_data,produces)

    # Identify unique countries
    unique_countries = combined_data["Country Code"].unique()

    # Base directory for country analysis
    country_analysis_dir = BLD_data / "Country_analysis"
    country_analysis_dir.mkdir(exist_ok=True)

    # # Iterate over each country and generate CSV files
    # for country in unique_countries:
    #     # Create country-specific folder
    #     country_folder = country_analysis_dir / str(country)
    #     country_folder.mkdir(exist_ok=True)

    #     # Filter data for the specific country
    #     country_data = combined_data[combined_data["Country Code"] == country]

    #     country_data["Abs_Diff_Perc"] = pd.to_numeric(country_data["Abs_Diff_Perc"], errors="coerce").fillna(0).astype(int)

    #     # Filter only outliers
    #     country_outliers = country_data[country_data["Abs_Diff_Perc"] > 50]

    #     # Save all data for the country
    #     country_data.to_csv(country_folder / f"{country}_net_incurrence_liabilities.csv", index=False)

    #     # Save outliers only for the country
    #     country_outliers.to_csv(country_folder / f"{country}_outliers_net_incurrence_liabilities.csv", index=False)

    print("Country-specific files generated successfully.")

