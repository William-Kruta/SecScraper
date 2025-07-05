import os

import re
import requests
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import time
import json
import pandas as pd

FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(FILE_DIR, "config.json")
print(f"FILE: {FILE_DIR}")


def read_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        data = json.load(f)
    return data


def get_mapping_dir() -> str:
    data = read_config()
    return data["mappings"]


def get_filings_dir() -> str:
    data = read_config()
    return data["filings"]


MAPPINGS_DIR = get_mapping_dir()
CACHE_DIR = get_filings_dir()


class SecScraper:
    def __init__(
        self, mappings_dir: str, cache_dir: str, delay: int = 1, debug: bool = True
    ):
        self.mappings_dir = mappings_dir
        self.cache_dir = cache_dir
        self.delay = delay
        self.debug = debug
        self.forms = ["10-K", "10-Q"]

    def _does_mapping_exist(self, ticker_or_cik: str) -> bool:
        if os.path.exists(os.path.join(self.mappings_dir, f"{ticker_or_cik}.json")):
            return True
        else:
            return False

    def _read_mappings(self, ticker_os_cik: str) -> dict:
        with open(os.path.join(self.mappings_dir, f"{ticker_os_cik}.json"), "r") as f:
            data = json.load(f)
        return data

    def download_latest_filings(self, ticker_or_cik):
        url_mappings = {
            "10-K": {"base": "", "filings": {}},
            "10-Q": {"base": "", "filings": {}},
        }
        json_path = os.path.join(self.mappings_dir, f"{ticker_or_cik}.json")
        base_url = "https://www.sec.gov"
        search_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}&dateb=&owner=include&count=40&search_text="
        # search_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type={}&owner=exclude&count=1"
        headers = {
            "User-Agent": "MyCompanyName EDGAR scraper (email@example.com)"  # per SEC policy
        }

        if not self._does_mapping_exist(ticker_or_cik):
            for form in self.forms:
                # 1. Get the search results page for the given form type
                url = search_url.format(ticker_or_cik, form)
                url_mappings[form]["base"] = url
                resp = requests.get(url, headers=headers)
                if resp.status_code != 200:
                    print(f"Failed to retrieve {form} page for {ticker_or_cik}")
                    continue
                soup = BeautifulSoup(resp.content, "html.parser")
                a_tags = soup.find_all("a", {"id": "documentsbutton"})
                urls = []
                # Extract urls from <a> tags.
                for a in a_tags:
                    href = a.get("href")
                    urls.append(href)
                # Find the "Documents" link for the first (latest) filing
                for u in urls:
                    new_url = f"{base_url}{u}"
                    # url_mapping[form]["documents"].append(new_url)
                    new_resp = requests.get(new_url, headers=headers)
                    new_soup = BeautifulSoup(new_resp.content, "html.parser")

                    table = new_soup.find("table", {"class": "tableFile"})
                    divs = new_soup.find_all("div", {"class": "info"})
                    div_text = [d.text for d in divs]

                    filing_date = div_text[0]
                    period_of_report = div_text[-1]

                    # print(f"DIVS: {divs}")
                    table_links = []
                    filing_types = []
                    if table:
                        for row in table.find_all("tr"):
                            cells = row.find_all("td")
                            if cells:
                                filing_types.append(cells[1].text)
                                link = row.find("a")
                                if link:
                                    table_links.append(link.get("href"))
                    if len(table_links) == 0:
                        return
                    else:
                        filing_url = f"{base_url}{table_links[0]}"
                        # url_mapping[form]["filings"].append(filing_url)
                    try:
                        filing_type = filing_types[0]
                    except IndexError:
                        filing_type = "N/A"

                    url_mappings[form]["filings"][period_of_report] = {
                        "type": filing_type,
                        "filing_date": filing_date,
                        "url": filing_url,
                    }
            with open(json_path, "w") as f:
                json.dump(url_mappings, f, indent=4)

        else:
            url_mappings = self._read_mappings(ticker_or_cik)
        return url_mappings

    def mapping_to_scraper_pipeline(self, ticker_or_cik):
        if self.debug:
            print(f"[Search] Collecting urls... ")
        mappings = self.download_latest_filings(ticker_or_cik)
        paths = self._create_paths(ticker_or_cik)
        if self.debug:
            print(f"[Search] Urls Collected {mappings}")
        for form in self.forms:
            if self.debug:
                print(f"[Search] Searching {form}")
            filings = mappings[form]["filings"]
            for k1, v1 in filings.items():
                url = v1.get("url", "")

                if not self.filing_exists(ticker_or_cik, form, k1, paths=paths):
                    if self.debug:
                        print(f"[Search] Scraping: {url}")
                    text = self.fetch_website_text(url)
                    markdown = self.html_to_markdown(text)
                    self.save_filing(ticker_or_cik, markdown, form, k1, paths)
                    if self.debug:
                        print(f"[Search] Saved filing...")
                else:
                    if self.debug:
                        print(f"[Search] Record exists: {url}")
                time.sleep(self.delay)
        if self.debug:
            print(f"[Search] Scraping Complete")

    def filing_exists(
        self,
        ticker_or_cik: str,
        filing_type: str,
        period_of_report: str,
        paths: dict = {},
    ) -> bool:
        ticker_or_cik = ticker_or_cik.upper()
        if paths == {}:
            paths = self._create_paths(ticker_or_cik)
        file_name = f"{period_of_report}_{ticker_or_cik.upper()}.md"
        if filing_type.upper() == "10-K":
            path = os.path.join(paths["annual"], file_name)
        elif filing_type.upper() == "10-Q":
            path = os.path.join(paths["quarter"], file_name)
        return os.path.exists(path)

    def _create_paths(self, ticker_or_cik: str) -> dict:
        # Create cache dir.
        os.makedirs(CACHE_DIR, exist_ok=True)
        # Create necessary paths within cache.
        ticker_dir = os.path.join(CACHE_DIR, ticker_or_cik.upper())
        annual_dir = os.path.join(ticker_dir, "10-K")
        quarter_dir = os.path.join(ticker_dir, "10-Q")
        # Create remaining directories
        os.makedirs(ticker_dir, exist_ok=True)
        os.makedirs(annual_dir, exist_ok=True)
        os.makedirs(quarter_dir, exist_ok=True)
        paths = {
            "ticker": ticker_dir,
            "annual": annual_dir,
            "quarter": quarter_dir,
        }
        return paths

    def save_filing(
        self,
        ticker_or_cik: str,
        filing_text: str,
        filing_type: str,
        period_of_report: str,
        paths: dict = {},
    ):
        if paths == {}:
            paths = self._create_paths(ticker_or_cik)
        # Create file name.
        file_name = f"{period_of_report}_{ticker_or_cik.upper()}.md"
        if filing_type.upper() == "10-K":
            path = os.path.join(paths["annual"], file_name)
        elif filing_type.upper() == "10-Q":
            path = os.path.join(paths["quarter"], file_name)
        self._write_to_file(filing_text, path)

    def _write_to_file(self, text: str, file_path: str):
        with open(file_path, "w", encoding="utf-8") as md_file:
            md_file.write(text)

    def fetch_website_text(self, ix_url: str):
        headers = {"User-Agent": "MyScript/1.0 (you@example.com)"}
        r = requests.get(ix_url, headers=headers)
        r.raise_for_status()
        return r.text

    def html_to_markdown(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
            tag.decompose()
        content = soup.find(id="formDiv") or soup.body
        md_chunks = []
        for elem in content.find_all(recursive=False):
            name = elem.name.lower()
            # HEADINGS
            if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
                level = int(name[1])
                text = elem.get_text(strip=True)
                md_chunks.append(f"{'#' * level} {text}\n")
            # PARAGRAPHS
            elif name == "p":
                text = elem.get_text(strip=True)
                if text:
                    md_chunks.append(f"{text}\n")
            # LISTS
            elif name in ("ul", "ol"):
                for li in elem.find_all("li", recursive=False):
                    bullet = "-" if name == "ul" else "1."
                    md_chunks.append(f"{bullet} {li.get_text(strip=True)}\n")
                md_chunks.append("\n")
            # TABLES → pandas → markdown
            elif name == "table":
                try:
                    df = pd.read_html(StringIO(str(elem)), header=0)[0]
                    md_chunks.append(df.to_markdown(index=False))
                    md_chunks.append("\n")
                except ValueError:
                    # if pandas can’t parse it, fall back to raw text
                    md_chunks.append(elem.get_text(separator=" | ", strip=True))
                    md_chunks.append("\n")
            else:
                text = elem.get_text(strip=True)
                if text:
                    md_chunks.append(f"{text}\n")
        md = "\n".join(chunk.strip() for chunk in md_chunks if chunk.strip())
        while "\n\n\n" in md:
            md = md.replace("\n\n\n", "\n\n")
        return md


if __name__ == "__main__":

    # print(f"DF: {df}")
    sec = SecScraper(MAPPINGS_DIR, CACHE_DIR, delay=0)

    sec.mapping_to_scraper_pipeline("ASTS")
