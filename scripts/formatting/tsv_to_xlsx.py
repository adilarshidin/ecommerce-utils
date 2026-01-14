import pandas as pd

INPUT_CSV = "output/konus_amazon_ready.tsv"
OUTPUT_XLSX = "output/konus_amazon_ready.xlsx"
CHUNK_SIZE = 100_000

def main():
    writer = pd.ExcelWriter(
        OUTPUT_XLSX,
        engine="openpyxl"
    )

    start_row = 0
    header_written = False

    # Specify the tab separator and encoding
    for chunk in pd.read_csv(INPUT_CSV, sep='\t', chunksize=CHUNK_SIZE, encoding='utf-8'):
        # Optional: clean MSRP column
        if 'MSRP' in chunk.columns:
            chunk['MSRP'] = chunk['MSRP'].str.replace('€', '').str.replace(' ', '').str.replace(',', '.')
        if 'Standard Price' in chunk.columns:
            chunk['Standard Price'] = chunk['Standard Price'].str.replace('€', '').str.replace(' ', '').str.replace(',', '.')

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
