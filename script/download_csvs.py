import os
import requests
from bs4 import BeautifulSoup
import zipfile

URL = "https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf"
URL_DRIVE = "https://drive.google.com/uc?export=download"
DIR_ZIP = "data_acidentes_prf_zip"
URL_DRIVE_FILE = "drive.google.com/file/d/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DIR_DATA_CSVS = "data_acidentes_prf"

def create_dir(path: str) -> bool:
    if(os.path.isdir(path)):
        print(f"The directory {path} aready exists")
        return False
    os.makedirs(path)
    print(f"Directory {path} create with sucess")
    return True

def extract_drive_id(url: str) -> str:
    return url.split("/d/")[1].split("/")[0]
    
def download_drive_file(file_id: str, destination: str, session = requests.Session()):
    URL_DRIVE = "https://drive.google.com/uc?export=download"

    response = session.get(URL_DRIVE, params={"id": file_id}, stream=True, timeout=30)

    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            response = session.get(
                URL_DRIVE,
                params={"id": file_id, "confirm": value},
                stream=True
            )

    with open(destination, "wb") as f:
        for chunk in response.iter_content(32768):
            if chunk:
                f.write(chunk)

def extract_zip(zip_path:str, path_extract:str) -> None:
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            zip_file.extractall(path_extract)
        print(f"{zip_path} extract to {path_extract}")
    except Exception as e:
        print(f"Error in extracting zip - {e}")

def request_page(url, headers=None) -> BeautifulSoup:
    try:
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.text, "html.parser")
        return soup
    
    except Exception as e:
        print(f"Error - {e}")

def get_drive_links(soup: BeautifulSoup) -> list[str]:
    drive_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if URL_DRIVE_FILE in href:
            drive_links.append(href)
    return drive_links

def number_files(drive_links: list[str]) -> None:
    print(f"Número de arquivos - {len(drive_links)}")

def main() -> None:
    response = request_page(URL, HEADERS)
    drive_links = get_drive_links(response)
    number_files(drive_links)
    create_dir(DIR_ZIP)
    create_dir(DIR_DATA_CSVS)
    for link in drive_links:
        file_id = extract_drive_id(link)
        file_name = f"{file_id}.zip"
        path = os.path.join(DIR_ZIP, file_name)
        print(f"downloading file {file_name}")
        download_drive_file(file_id, path)
        extract_zip(path, DIR_DATA_CSVS)

if __name__ == "__main__":
    main()
    