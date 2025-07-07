import os
import json
import pandas as pd
import datetime as dt
from edgar import Company, set_identity
from sec_edgar_downloader import Downloader


FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(FILE_DIR, "config.json")


def read_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        data = json.load(f)
    return data


def get_etf_dir() -> str:
    data = read_config()
    return data["etfs"]


def get_mapping_dir() -> str:
    data = read_config()
    return data["mappings"]


def get_filings_dir() -> str:
    data = read_config()
    return data["filings"]


def get_details_dir() -> str:
    data = read_config()
    return data["details"]


def get_edgar_dir() -> str:
    data = read_config()
    return data["edgar"]


def load_etf_ticker(ticker: str):
    etf_dir = get_etf_dir()
    etf_path = os.path.join(etf_dir, f"{ticker}.csv")
    df = pd.read_csv(etf_path)
    tickers = df["Symbol"].to_list()
    formatted_tickers = []
    edge_case = ["DFS"]
    for t in tickers:
        if t == "--":
            pass
        elif t in edge_case:
            pass
        else:
            if "." in t:
                t = t.replace(".", "-")
            elif "/" in t:
                t = t.replace("/", "-")
            formatted_tickers.append(t)
    return formatted_tickers


class SecScraper:
    def __init__(self, download_dir: str = "", debug: bool = True):
        self.debug = debug
        if download_dir == "":
            self.download_dir = get_filings_dir()
        else:
            self.download_dir = download_dir
        print(self.download_dir)
        self.downloader = Downloader(
            "indigo", "bob.botathan@indigo.com", self.download_dir
        )
        set_identity("bob.botathan@indigo.com")
        self.stale_states = {"details": 90}

    def _download_filings(self, ticker: str, form: str, limit=None):
        self.downloader.get(form, ticker, limit=limit)

    def _download_multiple_filing_types(
        self, ticker: str, forms: list = ["10-K", "10-Q"], limit=None
    ):
        for form in forms:
            if self.debug:
                print(f"-- Downloading '{form}' for {ticker}")
            self._download_filings(ticker, form, limit)

    def _get_existing_accn_numbers(
        self, ticker: str, form: str, full_path: bool = True
    ):
        ticker_dir = os.path.join(self.download_dir, ticker.upper())
        form_dir = os.path.join(ticker_dir, form.upper())
        filings = os.listdir(form_dir)
        if full_path:
            paths = [os.path.join(form_dir, filing) for filing in filings]
            return paths
        else:
            return filings

    def _get_existing_tickers(self) -> list:
        tickers = os.listdir(self.download_dir)
        return tickers

    def _get_existing_report_dates(self, ticker: str, form: str):
        accn_numbers = self._get_existing_accn_numbers(ticker, form, False)
        dates = []
        for a in accn_numbers:
            d = self._accn_number_to_dates(ticker, a)
            dates.append(d)
        return dates

    def _get_filing_details(self, ticker: str, form_type: str = ""):
        # Create necessary paths
        ticker_dir = os.path.join(self.download_dir, ticker.upper())
        path = os.path.join(ticker_dir, "filing_details.csv")

        try:
            filings = pd.read_csv(path, index_col=0)
            collected_date = filings["date_collected"].iloc[0]
            current_date = dt.datetime.now()
            delta = self._calc_delta(current_date, collected_date)
            if delta >= self.stale_states["details"]:
                company = Company(ticker)
                new_filings = company.get_filings().to_pandas()
                new_filings["date_collected"] = dt.datetime.now().date()
                combined = pd.concat([filings, new_filings], ignore_index=True)
                cleaned = combined.drop_duplicates(
                    subset=["accession_number"], keep="first", ignore_index=True
                )
                cleaned.to_csv(path)
                filings = cleaned
        except FileNotFoundError:
            company = Company(ticker)
            filings = company.get_filings().to_pandas()
            filings["date_collected"] = dt.datetime.now().date()
            filings.to_csv(path)
        if form_type != "":
            filings = filings[filings["form"] == form_type].reset_index(drop=True)
        return filings

    def _accn_number_to_dates(
        self, ticker: str, accn_number: str, target_col: str = "reportDate"
    ):
        filing_details = self._get_filing_details(ticker)
        matching_filing = filing_details[
            filing_details["accession_number"] == accn_number
        ]
        date = matching_filing[target_col].values[0]
        return date

    def _dates_to_accn_number(
        self, ticker: str, date: str, target_col: str = "reportDate"
    ):
        filing_details = self._get_filing_details(ticker)
        matching_filing = filing_details[filing_details[target_col] == date]
        return matching_filing["accession_number"].values[0]

    def _calc_delta(self, current_date, reference_date):
        date_format = "%Y-%m-%d"
        if isinstance(current_date, str):
            current_date = dt.datetime.strptime(current_date, date_format)
        if isinstance(reference_date, str):
            reference_date = dt.datetime.strptime(reference_date, date_format)

        delta = current_date - reference_date
        return delta.days

    def _read_file_to_string(self, file_path: str) -> str:
        """Reads a text file and returns its content as a string."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                file_content = file.read()
            return file_content
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_filings(self, ticker: str, form: str):
        ticker_dir = os.path.join(self.download_dir, ticker.upper())
        form_dir = os.path.join(ticker_dir, form)
        accn_numbers = self._get_existing_accn_numbers(ticker, form, False)
        mapping_data = {}
        for accn in accn_numbers:
            date = self._accn_number_to_dates(ticker, accn)
            filing_path = os.path.join(form_dir, accn)
            text_file = os.path.join(filing_path, "full-submission.txt")
            mapping_data[date] = self._read_file_to_string(text_file)
        return mapping_data


if __name__ == "__main__":
    tickers = load_etf_ticker("VTI")
    sec = SecScraper()
    tickers = sec.get_filings("RKLB", "10-K")
