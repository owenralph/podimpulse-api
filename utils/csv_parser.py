from io import StringIO
import pandas as pd
from typing import Any

def parse_csv(file_stream: Any) -> pd.DataFrame:
    """
    Parses the uploaded CSV file into a DataFrame.
    Args:
        file_stream (Any): File-like object or string containing CSV data.
    Returns:
        pd.DataFrame: Parsed DataFrame with 'Date' as datetime.
    Raises:
        ValueError: If the CSV cannot be parsed.
    """
    try:
        # Check if the input is already a string
        if isinstance(file_stream, str):
            csv_data = file_stream
        else:
            # Read and decode the binary stream
            csv_data = file_stream.read()
        # Parse the CSV data into a DataFrame
        return pd.read_csv(StringIO(csv_data), parse_dates=['Date'])
    except Exception as e:
        raise ValueError(f"Error parsing CSV file: {e}")
