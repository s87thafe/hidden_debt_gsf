"""All the general configuration of the project."""
from pathlib import Path

SRC = Path(__file__).parent.resolve()
BLD = SRC.joinpath("..", "..", "bld").resolve()

BLD_data = BLD / "data"
BLD_figures = BLD / "figures"
BLD_tables = BLD / "tables"

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()
PAPER_DIR = SRC.joinpath("..", "..", "paper").resolve()

__all__ = [
    "BLD",
    "SRC",
    "BLD_data",
    "BLD_figures",
    "BLD_tables",
    "TEST_DIR",
    "GROUPS",
]