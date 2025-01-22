from hidden_debt_gsf.config import BLD

# Define the subdirectories to create within BLD
files_produces = {
    "data": BLD / "data" / ".dir_created",
    "figures": BLD / "figures" / ".dir_created",
    "tables": BLD / "tables" / ".dir_created",
}

def task_create_directories(
    produces=files_produces
    ):
    # Define the subdirectories to create within BLD
    directories = {
    "data": BLD / "data",
    "figures": BLD / "figures",
    "tables": BLD / "tables",
    }
    for directory in directories.values():
        # Create the subdirectory if it doesn't exist
        directory.mkdir(parents=True, exist_ok=True)

    for file in produces.values():
        # Create a marker file in each subdirectory
        marker_file = file
        marker_file.touch()