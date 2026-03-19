""" 
The following code is for educational purposes only! 
This code scrapes the financial information from 100 companies, 
and then ranks them in ascending order in accordance with 3 predefined criterias, 
which will be defined later in the code. 
"""

import pandas as pd
import yfinance as yf
import numpy as np
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

pd.set_option('display.max_rows', 10)

def safe_get(df, row_name, col_index, default=np.nan):
    if df is None or df.empty:
        return default

    if row_name in df.index and col_index < len(df.columns):
        val = df.loc[row_name].iloc[col_index]
        return default if pd.isna(val) else val
    return default

def fetch_ticker(ticker):
    try:
        cpy = yf.Ticker(ticker)
        bs = cpy.balance_sheet
        inc = cpy.income_stmt
        cfs = cpy.cashflow

        # Compile the current and prior-year financial data into a structured record
        return {
            "tickers": ticker,
            "date": bs.columns[0] if not bs.empty and len(bs.columns) > 0 else np.nan,
            "date_yb": bs.columns[1] if not bs.empty and len(bs.columns) > 1 else np.nan,

            "totalAssets": safe_get(bs, "Total Assets", 0),
            "totalRevenue": safe_get(inc, "Total Revenue", 0),
            "totalRevenue_yb": safe_get(inc, "Total Revenue", 1),
            "netIncome": safe_get(inc, "Net Income", 0),
            "netIncome_yb": safe_get(inc, "Net Income", 1),

            "capEx": safe_get(cfs, "Capital Expenditure", 0, default=0),
            "capEx_yb": safe_get(cfs, "Capital Expenditure", 1, default=0),
            "depAmor": safe_get(cfs, "Depreciation And Amortization", 0, default=0),
            "depAmor_yb": safe_get(cfs, "Depreciation And Amortization", 1, default=0),

            "goodwill": safe_get(bs, "Goodwill", 0, default=0),
            "goodwill_yb": safe_get(bs, "Goodwill", 1, default=0),
            "otherIntang": safe_get(bs, "Other Intangible Assets", 0, default=0),
            "otherIntang_yb": safe_get(bs, "Other Intangible Assets", 1, default=0)
        }

    except Exception as e:
        print(f"  -> Error processing {ticker}: {e}. Filling with blanks.")
        return {
            "tickers": ticker, "date": np.nan, "date_yb": np.nan,
            "totalAssets": np.nan, "totalRevenue": np.nan, "totalRevenue_yb": np.nan,
            "netIncome": np.nan, "netIncome_yb": np.nan,
            "capEx": np.nan, "capEx_yb": np.nan, "depAmor": np.nan, "depAmor_yb": np.nan,
            "goodwill": np.nan, "goodwill_yb": np.nan, "otherIntang": np.nan, "otherIntang_yb": np.nan
        }

# Get SEC Data
headers = {'User-Agent': "christofferdej.acct@gmail.com"}
companyTickers = requests.get(
    "https://www.sec.gov/files/company_tickers.json",
    headers=headers
)

companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')
companyData['cik_str'] = companyData['cik_str'].astype(str).str.zfill(10)

tickers = companyData["ticker"][0:10].tolist()

BATCH_SIZE = 50
WORKERS = 5
DELAY_BETWEEN_BATCHES = 2  # seconds

financial_data = []
for i in range(0, len(tickers), BATCH_SIZE):
    batch = tickers[i:i + BATCH_SIZE]
    print(f"Fetching tickers {i+1}–{i+len(batch)} of {len(tickers)}...")
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        financial_data.extend(executor.map(fetch_ticker, batch))
    if i + BATCH_SIZE < len(tickers):
        time.sleep(DELAY_BETWEEN_BATCHES)
    
df = pd.DataFrame(financial_data)
df.set_index("tickers", inplace=True)

df.dropna(axis=0, inplace=True)
df["fcf"] = (df["netIncome"] + df["depAmor"] + df["capEx"])
df["fcf_yb"] = (df["netIncome_yb"] + df["depAmor_yb"] + df["capEx_yb"])
df["ROA"] = round((df["fcf"] / (df["totalAssets"] - df["goodwill"] - df["otherIntang_yb"])) * 100, 1)

filt = (df["fcf"] > 0)
df = df[filt]
filt1 = (df["fcf_yb"] > 0)
df = df[filt1]
filt2 = (df["fcf"] > df["fcf_yb"])
df = df[filt2]
filt3 = (df["totalRevenue"] > df["totalRevenue_yb"]) 
df = df[filt3]

df["fcf_growth"] = round(((df["fcf"] - df["fcf_yb"]) / df["fcf_yb"]) * 100, 1) 
df["revenue_growth"] = round(((df["totalRevenue"] - df["totalRevenue_yb"]) / df["totalRevenue_yb"]) * 100, 1)

df["ROA_rank"] = df["ROA"].rank(ascending=False) 
df["revenue_rank"] = df["revenue_growth"].rank(ascending=False) 
df["fcf_rank"] = df["fcf_growth"].rank(ascending=False)

df["points"] = df["ROA_rank"] + df["revenue_rank"] + df["fcf_rank"] 
df = df.sort_values(by="points", ascending=True)
df["ranking"] = range(1, len(df) + 1)

df = df[["date", "date_yb", "revenue_growth", "fcf_growth", "ROA", "ranking"]]
pd.set_option('display.max_rows', 100)

"""
Of the 100 companies that were scrapped, 47 are left
after the code have filtered through companies
that dident meet certain creteria, like for example having negative free cash 
flow or having missing values.
Is has then ranked all these companies by 3 creterias, these were:
1) Free cash flow growth: Compares how much the Free Cash Clow has grown from
   the year before in percent
2) Revenue growth : Comapares how much the Revenue has grown from the year
   before in percent
3) Return on Assets(ROA): Free cash flow / assets --> expressed as a percent
   
The program then sorts the companies by whose got the best combination of 
these creterias in ascending order. Scroll down to see result. 
"""
print(df.head())