import requests
import os
import zipfile

def download_file(url:str, save_path:str) -> None:
    try:
        response = requests.get(url)

        if response.status_code == 200:
            with open(save_path, "wb") as file:
                file.write(response.content)
            print(F"File saved in {save_path}")

        elif response.status_code != 200:
            print(f"Faile to save the file in {save_path}")
    except Exception as e:
        print(f"Error - {e}")

def extract_zip(zip_path:str, path_extract:str) -> None:
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            zip_file.extractall(path_extract)
        print(f"{zip_path} extract to {path_extract}")
    except Exception as e:
        print(f"Error in extracting zip - {e}")

download_file("https://drive.google.com/uc?export=download&id=1-G3MdmHBt6CprDwcW99xxC4BZ2DU5ryR"
, "teste.csv")

extract_zip("teste.csv", "teste_sem_zip.csv")