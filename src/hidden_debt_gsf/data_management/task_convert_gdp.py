import pandas as pd
from pathlib import Path
from hidden_debt_gsf.config import SRC, BLD_data, BLD_figures

def task_convert_country_codes(
        depends_on=SRC / "data" / "gdp_dta" / "gdp_lcu.dta",
        produces=SRC / "data" / "gdp_dta" / "gdp_lcu_convert.csv"
):
    """
    Converts the country codes in the GDP data from numeric to string format.

    Args:
        depends_on (dict): Dictionary containing dependencies, in this case, 
                           the path to the GDP data.
        produces (str): Path to the output .dta file.
    """
    # Read the GDP data
    gdp_data = pd.read_stata(depends_on)

    ## Load the country code conversion table
    #country_code_conversion = pd.read_csv(SRC / "country_code_conversion.csv")
#
    ## Merge the GDP data with the country code conversion table
    #gdp_data = gdp_data.merge(country_code_conversion, left_on="Country Code", right_on="Numeric Code")
#
    ## Drop the numeric country code column
    #gdp_data = gdp_data.drop(columns=["Numeric Code"])

    # Save the converted data
    gdp_data.to_csv(produces, index=False)
