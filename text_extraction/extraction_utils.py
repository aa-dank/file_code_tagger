# text_extraction/extraction_utils.py

from pathlib import Path
from bs4 import BeautifulSoup
from contextlib import contextmanager
import pythoncom, win32com.client
import subprocess, tempfile

def validate_file(path: str) -> Path:
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(path)
    return p

def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())

def strip_html(html: str, parser: str = "lxml", remove_tags=None) -> str:
    if remove_tags is None:
        remove_tags = ["script", "style", "noscript"]
    soup = BeautifulSoup(html, parser)
    for t in soup(remove_tags):
        t.decompose()
    return normalize_whitespace(soup.get_text(separator=" ", strip=True))

def run_pandoc(src: str, pandoc_path: str, to_format: str = "plain") -> Path:
    out = Path(tempfile.mkdtemp()) / (Path(src).stem + ".txt")
    cmd = [pandoc_path, src, "-t", to_format, "-o", str(out)]
    subprocess.run(cmd, check=True)
    return out

@contextmanager
def com_app(dispatch_name: str, visible: bool = False):
    pythoncom.CoInitialize()
    app = win32com.client.DispatchEx(dispatch_name)
    app.Visible = visible
    try:
        yield app
    finally:
        app.Quit()
        pythoncom.CoUninitialize()