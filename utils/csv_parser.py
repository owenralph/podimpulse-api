from io import StringIO
import pandas as pd
from typing import Any
from utils import handle_errors
import logging

@handle_errors
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
    logging.debug("Parsing CSV input stream.")
    try:
        # Check if the input is already a string
        if isinstance(file_stream, str):
            csv_data = file_stream
        else:
            # Read and decode the binary stream
            csv_data = file_stream.read()
        # Parse the CSV data into a DataFrame
        # Try default, then try semicolon delimiter with stripped quotes if needed
        try:
            return pd.read_csv(StringIO(csv_data), parse_dates=['Date'])
        except Exception:
            # Try semicolon delimiter and handle quoted headers/values
            try:
                df = pd.read_csv(StringIO(csv_data), delimiter=';', dtype=str)
                # Remove quotes from column names if present
                df.columns = [col.strip('"') for col in df.columns]
                # Remove quotes from all string columns
                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].str.strip('"')
                # Convert 'Date' to datetime if present
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                # Convert numeric columns if possible
                for col in df.columns:
                    if col != 'Date':
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except Exception:
                            pass
                return df
            except Exception as e2:
                logging.error(f"Error parsing CSV file: {e2}")
                raise ValueError(f"Error parsing CSV file: {e2}")
    except Exception as e:
        logging.error(f"Error parsing CSV file: {e}")
        raise ValueError(f"Error parsing CSV file: {e}")
