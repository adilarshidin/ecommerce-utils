# Amazon Utils

## Features

1. EAN to ASIN converter using [Rocketsource](https://app.rocketsource.io/).
2. Product names translator using Mistral LLM API.
3. Scraper for amazon inventory prices actualizer.

    Amazon inventory download shows incorrect prices for some items. This scraper

## Setup

1. Provide env variables.
2. Provide constants in the necessary script in `scripts/`.
3. Provide files to work with in `input` directory of the project root.

## Pipelines

1. Filtering spreadsheets:

    1) EAN-ASIN conversion. Check US and ES markets.

    2) Product names translation.

2. Amazon listing prices update:

    1) Convert txt to csv.

    2) Scrape actual prices from Amazon because the downloaded listing reports contain incorrect prices.

    3) Update using another csv.

## Tools needed

1. Scraper of item prices sold by other shops to find the reasonable margin.
2. Seller Assistant like info for finding best selling items and other insights.
