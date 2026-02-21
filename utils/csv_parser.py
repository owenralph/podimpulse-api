from io import StringIO
import pandas as pd
from typing import Any
import re
from utils import handle_errors, require_columns
import logging


_COLUMN_ALIASES = {
    "Date": {
        "date",
        "day",
        "datetime",
        "timestamp",
        "reportdate",
        "recordeddate",
        "downloaddate",
        "download_date",
        "periodstart",
        "period_start",
        "startdate",
    },
    "Downloads": {
        "downloads",
        "downloads_total",
        "totaldownloads",
        "total_downloads",
        "downloadcount",
        "download_count",
        "downloadscount",
        "downloads_count",
        "numberofdownloads",
        "number_of_downloads",
    },
}


def _normalize_column_name(name: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _strip_surrounding_quotes(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip().strip('"')
    return value


def _canonicalize_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    mapped_targets = set()

    for col in df.columns:
        normalized = _normalize_column_name(col)
        for canonical, aliases in _COLUMN_ALIASES.items():
            alias_set = aliases | {_normalize_column_name(canonical)}
            if normalized in alias_set and canonical not in mapped_targets:
                rename_map[col] = canonical
                mapped_targets.add(canonical)
                break

    if rename_map:
        df = df.rename(columns=rename_map)

    required_columns = ["Date", "Downloads"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        available = ", ".join(str(c) for c in df.columns)
        raise ValueError(
            "CSV must include columns for Date and Downloads. "
            f"Missing: {missing}. Available columns: [{available}]"
        )
    return df


def _coerce_downloads(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _coerce_dates(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    unresolved = parsed.isna()
    if unresolved.any():
        parsed.loc[unresolved] = pd.to_datetime(
            series.loc[unresolved],
            utc=True,
            errors="coerce",
            dayfirst=True,
        )
    return parsed


def _resample_dataframe_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["Date"] = pd.to_datetime(working["Date"], utc=True, errors="coerce")
    working = working.dropna(subset=["Date"]).sort_values("Date")
    if working.empty:
        return working

    working["Date"] = working["Date"].dt.normalize()
    working = working.set_index("Date")

    numeric_cols = list(working.select_dtypes(include=["number"]).columns)
    non_numeric_cols = [col for col in working.columns if col not in numeric_cols]

    agg_map = {col: "mean" for col in numeric_cols}
    agg_map.update({col: "first" for col in non_numeric_cols})
    grouped = working.groupby(level=0).agg(agg_map)

    full_index = pd.date_range(
        start=grouped.index.min(),
        end=grouped.index.max(),
        freq="D",
        tz="UTC",
    )
    daily = grouped.reindex(full_index)

    for col in numeric_cols:
        daily[col] = daily[col].interpolate(method="time").ffill().bfill()
    for col in non_numeric_cols:
        daily[col] = daily[col].ffill().bfill()

    return daily.reset_index().rename(columns={"index": "Date"})


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
        if isinstance(file_stream, str):
            csv_data = file_stream
        else:
            csv_data = file_stream.read()
        if isinstance(csv_data, bytes):
            csv_data = csv_data.decode("utf-8")

        try:
            df = pd.read_csv(StringIO(csv_data), dtype=str)
        except Exception:
            try:
                df = pd.read_csv(StringIO(csv_data), delimiter=';', dtype=str)
            except Exception as e2:
                logging.error(f"Error parsing CSV file: {e2}")
                raise ValueError(f"Error parsing CSV file: {e2}")

        df.columns = [_strip_surrounding_quotes(col) for col in df.columns]
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(_strip_surrounding_quotes)

        df = _canonicalize_required_columns(df)
        df["Date"] = _coerce_dates(df["Date"])
        df["Downloads"] = _coerce_downloads(df["Downloads"])
        return df
    except Exception as e:
        logging.error(f"Error parsing CSV file: {e}")
        raise ValueError(f"Error parsing CSV file: {e}")


@handle_errors
def validate_downloads_dataframe(
    downloads_df: pd.DataFrame,
    min_rows: int = 14,
    max_median_interval_days: float = 3.0,
    frequency_mode: str = "strict",
) -> pd.DataFrame:
    """
    Validates and normalizes parsed download data for downstream analytics.

    Args:
        downloads_df (pd.DataFrame): Parsed DataFrame.
        min_rows (int): Minimum usable rows required.
        max_median_interval_days (float): Largest acceptable median interval between records.

    Returns:
        pd.DataFrame: Cleaned and sorted DataFrame.
    """
    allowed_frequency_modes = {"strict", "resample_daily"}
    if frequency_mode not in allowed_frequency_modes:
        raise ValueError(
            f"Invalid frequency_mode '{frequency_mode}'. "
            f"Allowed values: {sorted(allowed_frequency_modes)}."
        )

    require_columns(downloads_df, ["Date", "Downloads"])

    df = downloads_df.copy()
    df["Date"] = _coerce_dates(df["Date"])
    df["Downloads"] = _coerce_downloads(df["Downloads"])
    df = df.dropna(subset=["Date", "Downloads"]).sort_values("Date").reset_index(drop=True)

    unique_dates = df["Date"].dt.normalize().drop_duplicates().sort_values()
    if len(unique_dates) < 2:
        raise ValueError("CSV must include at least two distinct dates.")

    intervals = unique_dates.diff().dropna().dt.total_seconds().div(86400)
    median_interval_days = float(intervals.median())

    if median_interval_days > max_median_interval_days:
        if frequency_mode == "resample_daily":
            if len(unique_dates) < 4:
                raise ValueError(
                    "Detected non-daily data cadence, but not enough periods to resample reliably. "
                    "Provide at least 4 dated periods or daily data."
                )
            df = _resample_dataframe_to_daily(df)
            warning = (
                "Input cadence appears non-daily (about "
                f"{median_interval_days:.1f} days between records). "
                "Data was resampled to daily values for modeling; treat trend/regression/prediction "
                "results as lower-confidence."
            )
            df.attrs["input_frequency_warning"] = warning
        else:
            raise ValueError(
                "Detected approximately "
                f"{median_interval_days:.1f} days between records. "
                "This API currently supports daily or near-daily download rows; "
                "weekly/monthly cadence will produce unreliable trend, regression, and prediction outputs. "
                "Use frequency_mode='resample_daily' to proceed with lower-confidence resampled data."
            )

    if len(df) < min_rows:
        raise ValueError(
            f"CSV contains {len(df)} usable rows after cleaning. "
            f"At least {min_rows} daily rows are recommended for reliable analysis."
        )

    return df
