# market-lens-data

A Python data pipeline that fetches financial data (income statements, balance sheets, cash flow) from Yahoo Finance and ticker metadata from the SEC. Data is validated with Pydantic and stored in Supabase (PostgreSQL), powering the [Market Lens](https://github.com/tottedj/market-lens) frontend.

## Tech Stack

- **Python** — core language
- **Supabase** — PostgreSQL database via PostgREST
- **Pydantic** — data validation and schema modeling
- **yfinance** — Yahoo Finance market data
- **Tenacity** — retry logic for external API calls

## Related

- **Frontend**: [market-lens](https://github.com/tottedj/market-lens) (Next.js / React / TypeScript)
- **Live demo**: [market-lens-liart.vercel.app](https://market-lens-liart.vercel.app)

## Disclaimer

This project uses data from Yahoo Finance and the SEC for **educational and personal learning purposes only**. It is not intended for commercial use, redistribution, or financial advice. All data belongs to its respective owners. Use of Yahoo Finance data is subject to their [Terms of Service](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html).