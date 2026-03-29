## Helper functions for loading Keithley CLI measurement data from Nextcloud shares.
import io
import zipfile

import pandas as pd  # type: ignore
import requests  # type: ignore
from pandas.errors import ParserError  # type: ignore

# Public Nextcloud share URL for testing; replace with your own when calling the function.
# This needs to be changed to the actual measurement data link when running the notebook, as the current one is just an example.
NEXTCLOUD = "https://nextcloud.jyu.fi/index.php/s/ogPiHFo9dCbQinA"


def _read_one_txt(raw_file, encoding="utf-8", sep="\t", comment="#"):
    """Read one txt stream with parser fallbacks for mixed-delimiter files."""
    text = io.TextIOWrapper(raw_file, encoding=encoding)

    # Try user-provided/default separator first.
    try:
        text.seek(0)
        return pd.read_csv(
            text,
            sep=sep,
            engine="python",
            comment=comment,
            on_bad_lines="error",
        )
    except ParserError:
        pass

    # Fallback 1: auto-detect delimiter and skip malformed rows.
    text.seek(0)
    try:
        return pd.read_csv(
            text,
            sep=None,
            engine="python",
            comment=comment,
            on_bad_lines="skip",
        )
    except ParserError:
        pass

    # Fallback 2: whitespace-delimited parsing and skip malformed rows.
    text.seek(0)
    return pd.read_csv(
        text,
        sep=r"\s+",
        engine="python",
        comment=comment,
        on_bad_lines="skip",
    )


def _read_one_csv(raw_file, encoding="utf-8"):
    metatags = (
        "SetupTitle",
        "TestParameter",
        "PrimitiveTest",
        "MetaData",
        "AnalysisSetup",
        "Dimension1",
        "Dimension2",
    )
    columntag = "DataName"
    datatag = "DataValue"

    text = io.TextIOWrapper(raw_file, encoding=encoding)

    # Read metatags until we find the column names
    metadata = {}
    data = []
    columns = []
    for line in text:
        if line.startswith(columntag):
            columns = line.strip().split(",")
            continue
        for tag in metatags:
            if line.startswith(tag):
                metadata[tag] = line.strip().split(",", 1)[1]
                continue
        if line.startswith(datatag):
            values = line.strip().split(",")
            if len(values) == len(columns):
                data.append(values)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=columns)

    return df


def nextcloud_txt_to_dataframe(
    public_share_url: str = NEXTCLOUD, sep="\t", encoding="utf-8", comment="#"
):
    """
    Download a public Nextcloud shared folder as ZIP and merge all .txt files into one DataFrame.

    Parameters
    ----------
    public_share_url : str
        Public share URL, e.g. https://cloud.example.com/s/SHARE_TOKEN
    sep : str or None
        Delimiter passed to pandas.read_csv; use "\t" for TSV.
    encoding : str
        Text encoding used to decode txt files.
    comment : str
        Comment character passed to pandas.read_csv.
    """
    download_url = public_share_url.rstrip("/") + "/download"

    response = requests.get(download_url, timeout=60)
    response.raise_for_status()

    frames = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        txt_files = [
            name for name in archive.namelist() if name.lower().endswith(".txt")
        ]

        for name in txt_files:
            with archive.open(name) as raw_file:
                df = _read_one_txt(
                    raw_file, encoding=encoding, sep=sep, comment=comment
                )
                if not df.empty:
                    df["source_file"] = name
                    frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def nextcloud_csv_to_dataframe(public_share_url: str = NEXTCLOUD, encoding="utf-8"):
    """
    Download a public Nextcloud shared folder as ZIP and merge all .csv files into one DataFrame.

    Parameters
    ----------
    public_share_url : str
        Public share URL, e.g. https://cloud.example.com/s/SHARE_TOKEN
    encoding : str
        Text encoding used to decode csv files.
    """
    download_url = public_share_url.rstrip("/") + "/download"

    response = requests.get(download_url, timeout=60)
    response.raise_for_status()

    frames = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        csv_files = [
            name for name in archive.namelist() if name.lower().endswith(".csv")
        ]

        for name in csv_files:
            with archive.open(name) as raw_file:
                df = _read_one_csv(raw_file, encoding=encoding)
                if not df.empty:
                    df["source_file"] = name
                    frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
