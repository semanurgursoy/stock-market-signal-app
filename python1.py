import os
import yfinance as yf
import numpy as np
import pandas as pd
import pandas_ta as ta
from ftplib import FTP

import warnings  
warnings.filterwarnings('ignore')

def get_nasdaqlisted():
    ftp = FTP('ftp.nasdaqtrader.com')               
    ftp.login(user="Anonymous", passwd="guest")    
    '230 Login successful.'
    ftp.cwd("/symboldirectory" )                    
    '250 Directory successfully changed.'
    with open('nasdaqlisted.txt', 'wb') as fp:
        ftp.retrbinary('RETR nasdaqlisted.txt', fp.write)
    '226 Transfer complete.'
    ftp.quit()
    '221 Goodbye.'

# Calculate RSI
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Generate Trading Signals
# 1 = BUY, -1 = SELL
def generate_RSI_signals(rsi_values):
    signals = []
    for rsi in rsi_values:
        if rsi > 70:
            signals.append(-1)
        elif rsi < 30:
            signals.append(1)
        else:
            signals.append(0)
    return signals

def generate_BB_signals(data, BB_data):
    signals = []
    for i in range(len(data)):
        if data.iloc[i]['Close'] <= BB_data.iloc[i]['BBL_20_2.0']: signals.append(1)
        elif data.iloc[i]['Close'] >= BB_data.iloc[i]['BBU_20_2.0']: signals.append(-1)
        else: signals.append(0)
    return signals

def generate_MFI_signals(mfi_values):
    signals = []
    for mfi in mfi_values:
        if mfi > 70:
            signals.append(-1)
        elif mfi < 30:
            signals.append(1)
        else:
            signals.append(0)
    return signals

def get_tickers():
    file = open("nasdaqlisted.txt", "r")

    file_length = len(file.readlines())
    tickers = []

    file = open("nasdaqlisted.txt", "r")
    for i in range(file_length):
        ticker = file.readline().split('|')[0]
        tickers.append(ticker)

    tickers.pop(file_length-1)
    tickers.pop(0)

    return tickers

def get_signals(data, RSI_period, BB_period, MFI_period):
    # Calculate RSI and generate signals
    rsi_values = ta.rsi(data['Close'], length = RSI_period)
    if isinstance(rsi_values, type(None)):
        return 
    data['RSI'] = rsi_values
    data['RSI_Signal'] = generate_RSI_signals(rsi_values)

    # Calculate BB and generate signals
    BB_data = ta.bbands(data.Close, length = BB_period)
    if isinstance(BB_data, type(None)) or isinstance(BB_data, type(None)):
        return
    data['BBL'] = BB_data['BBL_20_2.0']
    data['BBU'] = BB_data['BBU_20_2.0']
    data['BB_Signal'] = generate_BB_signals(data, BB_data)

    # Calculate MFI and generate signals
    mfi_values = ta.mfi(high = data.High, low = data.Low, close = data.Close, volume = data.Volume, length = MFI_period)
    data['MFI'] = mfi_values
    data['MFI_Signal'] = generate_MFI_signals(mfi_values)

    return data

def get_signed_data(data, ticker):
    rsi = data[(data['RSI_Signal'] == 1) | (data['RSI_Signal'] == -1)]
    rsi['ticker'] = ticker

    bb = data[(data['BB_Signal'] == 1) | (data['BB_Signal'] == -1)]
    bb['ticker'] = ticker

    mfi = data[(data['MFI_Signal'] == 1) | (data['MFI_Signal'] == -1)]
    mfi['ticker'] = ticker

    return rsi, bb, mfi    

def get_yesterday_buys_and_sells():

    df = pd.DataFrame()

    get_nasdaqlisted()
    tickers = get_tickers()
    
    RSI_period = 14
    BB_period = 20
    MFI_period = 20

    for i in tickers[2001:2002]:

        data = yf.download(i)
        data = data[-365:]

        signal_data =  get_signals(data, RSI_period, BB_period, MFI_period)
        if isinstance(signal_data, type(None)):
            continue
        else:
            data = signal_data
            
        rsi, bb, mfi = get_signed_data(data, i)

        df = pd.concat([df, rsi, bb, mfi], ignore_index=False)

    yesterday_date = pd.Timestamp("today").replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(1, unit='d')
    #df = df.where(df.index.to_series() == yesterday_date, inplace=False).dropna()


    # Filter Buy/Sell Signals by using RSI, BB and MFI combined
    signals = df[(df['MFI_Signal'] == 1) & (df['BB_Signal'] == 1) & (df['RSI_Signal'] == 1) | (df['MFI_Signal'] == -1) & (df['BB_Signal'] == -1) & (df['RSI_Signal'] == -1)]
    signals = signals.drop_duplicates()

    signals['Trade_Signal_Raw'] = signals['RSI_Signal'].map({ -1: 0, 1: 1})

    # Generate trading positions 
    signals['Trade_Position'] = signals['Trade_Signal_Raw'].diff()

    # Fix the first signal's Trade_Signal value due to diff() function outputing NaN since it's the first signal  
    signals['Trade_Position'].iloc[0] = signals['Trade_Signal_Raw'].iloc[0]
  
    # Due to restricting Pyramiding in our Trading Strategy, we filter buy and sell positions in a row  
    signals = signals[signals['Trade_Position'].isin([1.0, -1.0])]

    with pd.ExcelWriter(os.getcwd()+ "\\data\\" + yesterday_date.strftime("%Y-%m-%d") + '_df.xlsx', engine='xlsxwriter', datetime_format= "dd-mm-yyyy", date_format= "mmmm dd yyyy") as writer:
        df.to_excel(writer, sheet_name='Sheet_1', index=True, startrow=0)
    with pd.ExcelWriter(os.getcwd()+ "\\data\\" + yesterday_date.strftime("%Y-%m-%d") + '_signals.xlsx', engine='xlsxwriter', datetime_format= "dd-mm-yyyy", date_format= "mmmm dd yyyy") as writer:
        signals.to_excel(writer, sheet_name='Sheet_1', index=True, startrow=0)

    return signals

def backtest(signals):
    # Let's assume we have 100,000 USD as initial capital and at every signal we buy/sell one BTC
    initial_capital = float(100000)

    # Create an empty positions DataFrame with the same index as Signals_BTC to use as intermediary step
    positions = pd.DataFrame(index = signals.index)

    # Update the name 'Trade_Position' to 'Position'
    positions['Current_Position'] = signals['Trade_Position']

    portfolio = positions.multiply(signals['Close'], axis=0)
    portfolio['Next_Position'] = portfolio['Current_Position'].shift(-1)
    portfolio = portfolio.iloc[:-1 , :]
    portfolio['Current_Position_Size'] = portfolio['Current_Position'].abs()
    portfolio['Next_Position_Size'] = portfolio['Next_Position'].abs()
    portfolio['Gain/Loss'] = np.where(portfolio['Current_Position'] > 0, portfolio['Next_Position_Size'] - portfolio['Current_Position_Size'], portfolio['Current_Position_Size'] - portfolio['Next_Position_Size'])
    portfolio['Cumulative_Position_Size'] = portfolio['Current_Position_Size'].cumsum()
    portfolio['Cumulative_Gain/Loss'] = portfolio['Gain/Loss'].cumsum()
    portfolio['Total_Holdings'] = initial_capital + portfolio['Cumulative_Gain/Loss']
    portfolio['%_Return_per_Position'] = 100 * portfolio['Gain/Loss'] / portfolio['Current_Position_Size']
    portfolio['%_Return_Cumulative'] = 100 * portfolio['Cumulative_Gain/Loss'] / portfolio['Cumulative_Position_Size']
    
    # Cumulative Return of the Trading Stragey
    cumulative_return = round(portfolio.iloc[-1]['%_Return_Cumulative'], 2)

    # Number of profitable/unprofitable positions
    total_closed_positions = portfolio['Current_Position'].count()
    profitable_positions = portfolio[portfolio['%_Return_per_Position'] > 0]['%_Return_per_Position'].count()
    unprofitable_positions = portfolio[portfolio['%_Return_per_Position'] < 0]['%_Return_per_Position'].count()

    # Number of profitable positions divided by number of unprofitable positions
    win_ratio = round(profitable_positions/unprofitable_positions,2)

    # Number of profitable positions divided by total closed positions
    percent_profitable = 100 * round(profitable_positions/total_closed_positions,2)

    # Maximum Drawdown
    maximum_drawdown = round(portfolio['%_Return_Cumulative'].min(),2)

    print(f'Cumulative return of the Trading Stragey: {cumulative_return}%')
    print(f'Total number of closed positions: {total_closed_positions}')
    print(f'Number of profitable positions: {profitable_positions}')
    print(f'Number of unprofitable positions: {unprofitable_positions}')
    print(f'Profitability in terms of number of positions: {percent_profitable}%')
    print(f'Win Ratio: {win_ratio}')
    print(f'Maximum Drawdown {maximum_drawdown}%')
    

def write_to_db(df):

    conn = pg.connect(
            host='db',
            database='py-app-db',
            port=5432,
            user='postgres',
            password='postgres'
        )
    
    cursor = conn.cursor()
    print("Connection established.") 

    for i in range(len(df)):
        cursor.execute('''INSERT INTO py_outputs (datee, ticker, rsi, bbu, bbl, mfi, signal) VALUES (%s, %s, %s, %s, %s, %s, %s)''', 
                       (df.index[i].strftime("%Y-%m-%d"), df.iloc[i]['ticker'], df.iloc[i]['RSI'],  
                        df.iloc[i]['BBU'], df.iloc[i]['BBL'], df.iloc[i]['MFI'], df.iloc[i]['MFI_Signal'].astype(np.float64)))
        #d = cursor.fetchone()
        conn.commit()
    


import psycopg2 as pg
import traceback

try:
    signals = get_yesterday_buys_and_sells()
    #signals = pd.read_excel(r'data\2023-11-27_signals.xlsx')
    backtest(signals)
    #write_to_db(signals)
except Exception as e:
    print(traceback.format_exc())




    