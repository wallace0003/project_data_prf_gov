import pandas as pd
import os
import csv
import re
from sqlalchemy import create_engine, Engine
from dotenv import load_dotenv
from pandas.io.parsers import TextFileReader

load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
SCHEMA = os.getenv("schema")
PORT = os.getenv("port")
HOST = os.getenv("host")
DATABASE = os.getenv("database")
CSV_PATH = "data_acidentes_prf/"

def detect_delimiter(file_path: str, encoding="latin1") -> str:
    with open(file_path, "r", encoding=encoding, errors="ignore") as f:
        sample = f.read(4096)
        sniffer = csv.Sniffer()
        return sniffer.sniff(sample).delimiter

def files_in_path(path:str) -> list[str]:
    return [file for file in os.listdir(path) if file.endswith(".csv")]

def csvs_to_df(files_csvs: list[str]) -> list[pd.DataFrame]:
    dataframes = []

    for file in files_csvs:
        file_path = f"{CSV_PATH}{file}"
        sep = detect_delimiter(file_path)

        try:
            df = pd.read_csv(
                file_path,
                sep=sep,
                encoding="latin1",
                engine="python",
                on_bad_lines="skip",
                chunksize=900_000
            )

            dataframes.append(df)

        except Exception as e:
            print(f"❌ Falha ao ler {file}")
            print(f"Erro: {e}")

    return dataframes

def normalize_table_name(filename: str) -> str:
    name = filename.lower().replace(".csv", "")
    return re.sub(r"[^a-z0-9_]", "_", name)

def db_conection():
    engine = create_engine(
        f"{DATABASE}://{USER}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}"
    )
    return engine

def transfer_csvs_to_sql(engine: Engine, files: list[str], dfs:list[TextFileReader]) -> None:
    with engine.begin() as conn:
        for file_name, reader in zip(files, dfs):
            table_name = normalize_table_name(file_name)
            first_chunk = True

            print(f"📥 Importing {file_name} → table {table_name}")

            for df_chunk in reader:
                df_chunk.to_sql(
                    table_name,
                    conn,
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    method="multi",
                )
                first_chunk = False
            print(f"✅ Tabela {table_name} criada com sucesso")
 

def main() -> None:
    files = files_in_path(CSV_PATH)
    dfs = csvs_to_df(files)
    engine = db_conection()
    transfer_csvs_to_sql(engine=engine, files=files, dfs=dfs)

if __name__ == "__main__":
    main()
