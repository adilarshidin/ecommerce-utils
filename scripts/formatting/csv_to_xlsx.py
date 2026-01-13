import pandas as pd

INPUT_CSV = "output/amazon_updated_prices.csv"
OUTPUT_XLSX = "output/amazon_updated_prices.xlsx"

CHUNK_SIZE = 100_000


def main():
    writer = pd.ExcelWriter(
        OUTPUT_XLSX,
        engine="openpyxl"
    )

    start_row = 0
    header_written = False

    for chunk in pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE):
        chunk.to_excel(
            writer,
            index=False,
            startrow=start_row,
            header=not header_written
        )

        start_row += len(chunk)
        header_written = True

        print(f"Wrote {start_row} rows")

    writer.close()
    print(f"\nDONE: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
