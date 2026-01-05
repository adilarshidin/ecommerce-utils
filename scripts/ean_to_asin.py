import os
import csv
import requests
import pandas as pd
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://app.rocketsource.io"
CONVERT_URL = urljoin(BASE_URL, "api/v3/convert")

INPUT_CSV = "../input/catalog.csv"
SUCCESS_CSV = "../output/asin_results.csv"
FAILED_CSV = "../output/failed_eans.csv"
CHECKPOINT_FILE = "../input/checkpoint.txt"

CSV_CHUNK_SIZE = 5000
API_BATCH_SIZE = 20
REQUEST_TIMEOUT = 60


def fetch_asins(eans):
    payload = {
        "marketplace": "us",
        "ids": eans
    }

    response = requests.post(
        CONVERT_URL,
        headers={"Authorization": f"Bearer {os.getenv('TOKEN')}"},
        json=payload,
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.json()


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    with open(CHECKPOINT_FILE, "r") as f:
        return int(f.read().strip())


def save_checkpoint(row_index):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(row_index))


def main():
    if not os.getenv("TOKEN"):
        raise RuntimeError("TOKEN is not set in environment")

    start_row = load_checkpoint()
    print(f"Resuming from row {start_row}")

    reader = pd.read_csv(INPUT_CSV, chunksize=CSV_CHUNK_SIZE)

    success_file_exists = os.path.exists(SUCCESS_CSV)
    failed_file_exists = os.path.exists(FAILED_CSV)

    success_file = open(SUCCESS_CSV, "a", newline="", encoding="utf-8")
    failed_file = open(FAILED_CSV, "a", newline="", encoding="utf-8")

    success_writer = None
    failed_writer = None

    global_row_index = 0

    for df_chunk in reader:
        chunk_size = len(df_chunk)

        if global_row_index + chunk_size <= start_row:
            global_row_index += chunk_size
            continue

        if "EAN" not in df_chunk.columns:
            raise ValueError("CSV must contain an 'EAN' column")

        if success_writer is None:
            success_headers = list(df_chunk.columns) + ["ASIN"]
            failed_headers = list(df_chunk.columns) + ["FailureReason"]

            success_writer = csv.DictWriter(success_file, fieldnames=success_headers)
            failed_writer = csv.DictWriter(failed_file, fieldnames=failed_headers)

            if not success_file_exists:
                success_writer.writeheader()
            if not failed_file_exists:
                failed_writer.writeheader()

        df_chunk = df_chunk.reset_index(drop=True)

        for i in range(0, chunk_size, API_BATCH_SIZE):
            batch_rows = df_chunk.iloc[i:i + API_BATCH_SIZE]
            batch_eans = batch_rows["EAN"].astype(str).tolist()

            try:
                response_data = fetch_asins(batch_eans)
            except Exception as e:
                for _, row in batch_rows.iterrows():
                    failed_writer.writerow({
                        **row.to_dict(),
                        "FailureReason": f"API error: {str(e)}"
                    })
                global_row_index += len(batch_rows)
                save_checkpoint(global_row_index)
                continue

            for _, row in batch_rows.iterrows():
                ean = str(row["EAN"])
                asin_list = response_data.get(ean)

                if not asin_list or asin_list == ["No ASIN found"]:
                    failed_writer.writerow({
                        **row.to_dict(),
                        "FailureReason": "No ASIN found"
                    })
                else:
                    for asin in asin_list:
                        success_writer.writerow({
                            **row.to_dict(),
                            "ASIN": asin
                        })

                global_row_index += 1
                save_checkpoint(global_row_index)

        print(f"Processed {global_row_index} rows")

    success_file.close()
    failed_file.close()

    print("\nDONE")
    print(f"Results: {SUCCESS_CSV}")
    print(f"Failures: {FAILED_CSV}")
    print(f"Checkpoint saved at row {global_row_index}")


if __name__ == "__main__":
    main()
