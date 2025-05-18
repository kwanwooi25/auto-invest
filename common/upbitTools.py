import pyupbit
import time
from pprint import pprint
from typing import Literal

def getTotalPrincipal(balances: list({'avg_buy_price': float,
  'avg_buy_price_modified': bool,
  'balance': float,
  'currency': str,
  'locked': float,
  'unit_currency': str})) -> float:
  print("========== getTotalPrincipal Start ==========")
  pprint(balances)

  total = 0.0

  for balance in balances:
    try:
      if balance['currency'] == 'KRW':
        total += (float(balance['balance']) + float(balance['locked']))
      else:
        avgBuyPrice = float(balance['avg_buy_price'])
        if avgBuyPrice != 0 and (float(balance['balance']) != 0 or float(balance['locked']) != 0):
          realTicker = balance['unit_currency'] + "-" + balance['currency']
          time.sleep(0.1)
          currentPrice = pyupbit.get_current_price(realTicker)
          total += (float(currentPrice) * (float(balance['balance']) + float(balance['locked'])))
    except Exception as e:
      print("[ERROR]", e)

  print("> total:", total)
  print("========== getTotalPrincipal End ==========")
  return total

def getTotalMarketValue(balances: list({'avg_buy_price': float,
  'avg_buy_price_modified': bool,
  'balance': float,
  'currency': str,
  'locked': float,
  'unit_currency': str})) -> float:
  print("========== getTotalMarketValue Start ==========")
  pprint(balances)

  total = 0.0

  for balance in balances:
    try:
      if balance['currency'] == 'KRW':
        total += (float(balance['balance']) + float(balance['locked']))
      else:
        avgBuyPrice = float(balance['avg_buy_price'])
        if avgBuyPrice != 0 and (float(balance['balance']) != 0 or float(balance['locked']) != 0):
          realTicker = balance['unit_currency'] + "-" + balance['currency']
          time.sleep(0.1)
          currentPrice = pyupbit.get_current_price(realTicker)
          total += (float(currentPrice) * (float(balance['balance']) + float(balance['locked'])))
    except Exception as e:
      print("[ERROR]", e)

  print("> total:", total)
  print("========== getTotalMarketValue End ==========")
  return total

def getMovingAverages(ohlcv, period, tergetDate):
  close = ohlcv["close"]
  ma = close.rolling(period).mean()
  return float(ma.iloc[tergetDate])

def getTopCoinList(interval: Literal["day", "week", "month", "minute15", "minute10", "minute5", "minute1"] = "day", top: int = 10):
  print("========== getTopCoinList Start ==========")
  print("interval:", interval)
  print("top:", top)

  try:
    tickers = pyupbit.get_tickers("KRW")
    time.sleep(0.1)
    coinDict = dict()

    for ticker in tickers:
      try:
        time.sleep(0.05)
        df = pyupbit.get_ohlcv(ticker, interval)
        totalVolume = (df['close'].iloc[-1] * df['volume'].iloc[-1]) + (df['close'].iloc[-2] * df['volume'].iloc[-2])
        coinDict[ticker] = totalVolume
        print(ticker, ":", totalVolume)
      except Exception as e:
        print("[ERROR]", e)

    sortedCoinList = sorted(coinDict.items(), key=lambda x: x[1], reverse=True)
    filteredCoinList = list()

    count = 0
    for coin in sortedCoinList:
      count += 1
      if count <= top:
        filteredCoinList.append(coin[0])
      else:
        break

    print("> filteredCoinList:", filteredCoinList)
    print("========== getTopCoinList End ==========")

    return filteredCoinList
  except Exception as e:
    print("[ERROR]", e)
    return None

def getHasCoin(balances, ticker):
  hasCoin = False
  for balance in balances:
    realTicker = balance['unit_currency'] + "-" + balance['currency']
    if realTicker == ticker:
      hasCoin = True

  return hasCoin

def getAverageBuyPrice(balances, ticker):
  averageBuyPrice = 0
  for balance in balances:
    realTicker = balance['unit_currency'] + "-" + balance['currency']
    if realTicker == ticker:
      averageBuyPrice = float(balance['avg_buy_price'])

  return averageBuyPrice

def buyCoinOnMarketPrice(upbit, ticker, amount):
  time.sleep(0.05)
  print(upbit.buy_market_order(ticker, amount))
  time.sleep(2.0)
  balances = upbit.get_balances()
  return balances

def sellCoinOnMarketPrice(upbit, ticker, amount):
  time.sleep(0.05)
  print(upbit.sell_market_order(ticker, amount))
  time.sleep(2.0)
  balances = upbit.get_balances()
  return balances

def cancelAllOrders(upbit, ticker):
  orders = upbit.get_order(ticker)
  if len(orders) > 0:
    for order in orders:
      print(upbit.cancel_order(order['uuid']))
      time.sleep(0.1)