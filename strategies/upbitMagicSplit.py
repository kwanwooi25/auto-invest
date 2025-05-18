from pprint import pprint
from dotenv import load_dotenv
import os
import pyupbit
import time
import json
import logging
from common import upbitTools
from notifications import slack

logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')
streamHandler = logging.StreamHandler()
fileHandler = logging.handlers.RotatingFileHandler('./logs/upbitMagicSplit.log', maxBytes=1024*1024*100, backupCount=10)
streamHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)

load_dotenv()

UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")
APP_ENV = os.getenv("APP_ENV")
STRATEGY_NAME = 'UpbitMagicSplit'
JSON_FILE_PATH = './json/upbitMagicSplit.json' if APP_ENV == 'local' else '/apps/auto-invest/json/upbitMagicSplit.json'

def getInvestmentPlans(
  targetStockList: list,
  totalInvestmentAmount: float,
  installmentCount:int = 10):
  logger.info("========== getInvestmentPlans Start ==========")

  investmentList = list()

  for targetStock in targetStockList:
    logger.info(f"===== {targetStock['ticker']} ({targetStock['investmentRate'] * 100}%) =====")
    ticker = targetStock['ticker']
    investmentRate = targetStock['investmentRate']

    totalInvestmentAmountForTicker = totalInvestmentAmount * investmentRate

    firstInvestmentAmount = totalInvestmentAmountForTicker * 0.4
    remainingInvestmentAmount = totalInvestmentAmountForTicker * 0.6

    logger.info(f"> 1차수 할당 금액 {firstInvestmentAmount}")
    logger.info(f"> 나머지 차수 할당 금액 {remainingInvestmentAmount}")

    time.sleep(0.2)

    df = pyupbit.get_ohlcv(ticker, interval="day")
    # 전일 종가
    yesterdayClosingPrice = df['close'].iloc[-2]

    # 전전일 5일 이동평균
    ma5TwoDaysAgo = upbitTools.getMovingAverages(df, 5, -2)
    # 전일 5일 이동평균
    ma5Yesterday = upbitTools.getMovingAverages(df, 5, -1)
    # 전전일 20일 이동평균
    ma20TwoDaysAgo = upbitTools.getMovingAverages(df, 20, -2)
    # 전일 20일 이동평균
    ma20Yesterday = upbitTools.getMovingAverages(df, 20, -1)
    # 전전일 60일 이동평균
    ma60TwoDaysAgo = upbitTools.getMovingAverages(df, 60, -2)
    # 전일 60일 이동평균
    ma60Yesterday = upbitTools.getMovingAverages(df, 60, -1)

    minPrice = df['close'].min()
    maxPrice = df['close'].max()

    gap = maxPrice - minPrice
    stepGap = gap / installmentCount
    percentGap = round((gap / minPrice) * 100,2)

    logger.info(f"> 최근 200개 캔들 최저가: {minPrice}")
    logger.info(f"> 최근 200개 캔들 최고가: {maxPrice}")

    logger.info(f"> 최고 최저가 차이: {gap}")
    logger.info(f"> 각 간격 사이의 갭: {stepGap}")
    logger.info(f"> 분할이 기준이 되는 갭의 크기: {percentGap} %")

    targetRate = round((percentGap / installmentCount) * 100,2)
    triggerRate = -round((percentGap / installmentCount) * 100,2)

    logger.info(f"> 각 차수의 목표 수익률: {targetRate} %")
    logger.info(f"> 각 차수의 진입 기준이 되는 이전 차수 손실률: {triggerRate} %")

    #현재 구간을 구할 수 있다.
    currentStep = installmentCount

    for step in range(1,int(installmentCount)+1):

      if yesterdayClosingPrice < minPrice + (stepGap * step):
        currentStep = step
        break

    logger.info(f"> 현재 구간: {currentStep}")

    investmentPlans = list()

    for i in range(int(installmentCount)):
      order = i + 1

      # 1차수라면
      if order == 1:

        finalInvestRate = 0

        # 이동평균선에 의해 최대 60%!!
        if yesterdayClosingPrice >= ma5Yesterday:
          finalInvestRate += 10
        if yesterdayClosingPrice >= ma20Yesterday:
          finalInvestRate += 10
        if yesterdayClosingPrice >= ma60Yesterday:
          finalInvestRate += 10

        if ma5Yesterday >= ma5TwoDaysAgo:
          finalInvestRate += 10
        if ma20Yesterday >= ma20TwoDaysAgo:
          finalInvestRate += 10
        if ma60Yesterday >= ma60TwoDaysAgo:
          finalInvestRate += 10

        logger.info(f"> 1차수 진입 이동평균선에 의한 비율: {finalInvestRate} %")

        # 현재 분할 위치에 따라 최대 40%

        logger.info(f"> 1차수 진입 현재 구간에 의한 비율: {((int(installmentCount)+1)-currentStep) * (40.0/installmentCount)} %")
        finalInvestRate += (((int(installmentCount)+1)-currentStep) * (40.0/installmentCount))

        finalFirstInvestmentAmount = firstInvestmentAmount * (finalInvestRate/100.0)
        logger.info(f"> 1차수 진입 금액 {finalFirstInvestmentAmount} 할당 금액 대비 투자 비중: {finalInvestRate} %")

        investmentPlans.append({
          "order": order,
          "targetRate": targetRate,
          "triggerRate": triggerRate,
          "amount": round(finalFirstInvestmentAmount)
        })

      # 그 밖의 차수
      else:
        investmentPlans.append({
          "order": order,
          "targetRate": targetRate,
          "triggerRate": triggerRate,
          "amount": round(remainingInvestmentAmount / (installmentCount-1))
        })

    investmentList.append({
      "ticker": ticker,
      "investmentPlans": investmentPlans
    })

  logger.info(investmentList)
  logger.info("========== getInvestmentPlans End ==========")

  return investmentList

def loadOrGenerateMagicSplitListItem(investment: dict):
  logger.info("========== loadOrGenerateMagicSplitListItem Start ==========")
  ticker = investment['ticker']
  investmentPlans = investment['investmentPlans']

  magicSplitList = list()

  try:
    with open(JSON_FILE_PATH, 'r') as json_file:
      magicSplitList = json.load(json_file)
  except Exception as e:
    logger.error("[ERROR]", e)

  magicSplitData = None

  for magicSplit in magicSplitList:

    if magicSplit['ticker'] == ticker:
      magicSplitData = magicSplit
      break

  # 최초 매수인 경우
  if magicSplitData == None:
    magicSplitListItem = dict()
    magicSplitListItem['ticker'] = ticker
    magicSplitListItem['date'] = 0

    purchasePlans = list()

    for i in range(len(investmentPlans)):
      purchasePlans.append({
        "order": i + 1,
        "price": 0,
        "amount": 0,
        "hasBought": False,
      })

    magicSplitListItem['purchasePlans'] = purchasePlans
    magicSplitListItem['realizedPNL'] = 0

    magicSplitList.append(magicSplitListItem)

    message = f"[{STRATEGY_NAME}] [{ticker}] 투자 준비 완료!"
    logger.info(message)
    slack.sendMessage(message)

    try:
      with open(JSON_FILE_PATH, 'w') as json_file:
        json.dump(magicSplitList, json_file)
    except Exception as e:
      logger.error("[ERROR]", e)

    logger.info("========== loadOrGenerateMagicSplitListItem End ==========")

  return magicSplitList

def getInvestmentPlan(investmentPlans, order):
    selectedInvestmentPlan = None
    for investmentPlan in investmentPlans:
        if order == investmentPlan["order"]:
            selectedInvestmentPlan = investmentPlan
            break

    return selectedInvestmentPlan

def getPurchasePlan(magicSplitList, order):
    selectedPurchasePlan = None
    for magicSplit in magicSplitList:
        if order == magicSplit["order"]:
            selectedPurchasePlan = magicSplit
            break

    return selectedPurchasePlan

def calculateProfit(balances, ticker):
  balances = upbit.get_balances()
  time.sleep(0.04)

  profit = dict()
  profit['amount'] = 0
  profit['rate'] = 0

  for balance in balances:
    try:
      realTicker = balance['unit_currency'] + "-" + balance['currency']
      if ticker == realTicker:
        currentPrice = pyupbit.get_current_price(realTicker)
        profit['amount'] = (float(currentPrice) - float(balance['avg_buy_price'])) * upbit.get_balance(realTicker)
        profit['rate'] = (float(currentPrice) - float(balance['avg_buy_price'])) * 100.0 / float(balance['avg_buy_price'])
        time.sleep(0.06)
        break

    except Exception as e:
      logger.error("[ERROR]", e)

  return profit

upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)

dayInNumber = time.gmtime().tm_mday

balances = upbit.get_balances()

# 총 원금
totalPrincipal = upbitTools.getTotalPrincipal(balances)

# 총 평가금액
totalMarketValue = upbitTools.getTotalMarketValue(balances)

# 투자 종목 리스트
targetStockList = [
  {
    "ticker": "KRW-USDT",
    "investmentRate": 0.4,
  },
  {
    "ticker": "KRW-BTC",
    "investmentRate": 0.3,
  },
  {
    "ticker": "KRW-XRP",
    "investmentRate": 0.3,
  },
]

# 최소 매수 금액
minBuyAmount = 10000
# 투자 비중
totalInventmentRate = 1
# 총 투자 금액
totalInvestmentAmount = totalMarketValue * totalInventmentRate
# 분할 수
installmentCount = 10

investmentList = getInvestmentPlans(targetStockList, totalInvestmentAmount, installmentCount)

magicSplitList = list()

for investment in investmentList:
  ticker = investment['ticker']
  investmentPlans = investment['investmentPlans']

  currentPrice = pyupbit.get_current_price(ticker)

  magicSplitList = loadOrGenerateMagicSplitListItem(investment)

  for magicSplit in magicSplitList:
    if magicSplit['ticker'] == ticker:
      time.sleep(0.3)
      df = pyupbit.get_ohlcv(ticker, interval="day")
      logger.info(df)

      # 전일 시가
      yesterdayOpeningPrice = df['open'].iloc[-2]
      # 전일 종가
      yesterdayClosingPrice = df['close'].iloc[-2]
      # 전전일 5일 이동평균
      ma5TwoDaysAgo = upbitTools.getMovingAverages(df, 5, -2)
      # 전일 5일 이동평균
      ma5Yesterday = upbitTools.getMovingAverages(df, 5, -1)

      # 1차수 매수되지 않았으면 1차수 매수
      for purchasePlan in magicSplit['purchasePlans']:
        if purchasePlan['order'] == 1:
          if purchasePlan['hasBought'] == False and magicSplit['date'] != dayInNumber:
            if yesterdayOpeningPrice < yesterdayClosingPrice and (yesterdayClosingPrice >= ma5Yesterday or ma5TwoDaysAgo <= ma5Yesterday):
              # 누적 실현손익 초기화
              purchasePlan['realizedPNL'] = 0

              if upbitTools.getHasCoin(balances, ticker) == True:
                purchasePlan['hasBought'] = True
                purchasePlan['price'] = upbitTools.getAverageBuyPrice(balances, ticker)
                purchasePlan['amount'] = upbit.get_balance(ticker)

                message = f"[{STRATEGY_NAME}] [{ticker}] 기존 잔고로 1차 투자를 대체합니다!"
                logger.info(message)
                slack.sendMessage(message)
              else:
                # 1차수 투자 계획
                investmentPlan = getInvestmentPlan(investmentPlans, 1)

                # 매수 전 보유 수량
                existingCoinAmount = upbit.get_balance(ticker)

                amountToBuy = investmentPlan['amount']
                if (amountToBuy < minBuyAmount):
                  amountToBuy = minBuyAmount

                # 시장가 매수
                balances = upbitTools.buyCoinOnMarketPrice(upbit, ticker,amountToBuy)

                purchasePlan['hasBought'] = True
                purchasePlan['price'] = currentPrice
                purchasePlan['amount'] = abs(upbit.get_balance(ticker) - existingCoinAmount)

                message = f"[{STRATEGY_NAME}] [{ticker}] 1차 투자 완료!"
                logger.info(message)
                slack.sendMessage(message)

              with open(JSON_FILE_PATH, 'w') as json_file:
                json.dump(magicSplitList, json_file)
      else:
        if upbitTools.getHasCoin(balances, ticker) == False:
          purchasePlan['hasBought'] = False
          purchasePlan['price'] = 0
          purchasePlan['amount'] = 0

          with open(JSON_FILE_PATH, 'w') as json_file:
            json.dump(magicSplitList, json_file)

      for purchasePlan in magicSplit['purchasePlans']:
        selectedInvestmentPlan = getInvestmentPlan(investmentPlans, purchasePlan['order'])

        # 이미 매수한 차수
        if purchasePlan['hasBought'] == True:
          currentProfitRate = (currentPrice - purchasePlan['price']) / purchasePlan['price'] * 100.0

          print(f"{ticker} {purchasePlan['order']}차수 수익률: {round(currentProfitRate, 2)}% (목표수익률: {selectedInvestmentPlan['targetRate']}%)")

          profit = calculateProfit(balances, ticker)

          # 목표 수익률을 달성한 경우
          if currentProfitRate >= selectedInvestmentPlan['targetRate'] and upbitTools.getHasCoin(balances, ticker) == True:
            amountToSell = purchasePlan['amount']
            currentBalance = upbit.get_balance(ticker)

            isAmountToSellAdjusted = False
            # 보유 수량이 매도 수량보다 작은 경우 매도 수량 보정
            if amountToSell > currentBalance:
              amountToSell = currentBalance
              isAmountToSellAdjusted = True

            # 모든 주문 취소
            upbitTools.cancelAllOrders(upbit, ticker)
            time.sleep(0.2)

            if purchasePlan['order'] == 1:
              amountToSell = currentBalance

            # 시장가 매도
            balances = upbitTools.sellCoinOnMarketPrice(upbit, ticker, amountToSell)

            purchasePlan['hasBought'] = False
            magicSplit['realizedPNL'] += profit['amount'] * amountToSell / currentBalance

            message = f"[{STRATEGY_NAME}] [{ticker}] {purchasePlan['order']}차수 수익 매도 완료! 차수 목표수익률 {selectedInvestmentPlan['targetRate']}% 만족"

            if isAmountToSellAdjusted == True:
              message = f"[{STRATEGY_NAME}] [{ticker}] {purchasePlan['order']}차수 수익 매도 완료! 차수 목표수익률 {selectedInvestmentPlan['targetRate']}% 만족 매도할 수량이 보유 수량보다 많은 상태라 모두 매도함!"

            logger.info(message)
            slack.sendMessage(message)

            # 1차수 매도인 경우 오늘 날짜를 넣어서 오늘 다시 1차 매수가 되지 않도록 함
            # if purchasePlan['order'] == 1:
            #   purchasePlan['date'] = dayInNumber

            with open(JSON_FILE_PATH, 'w') as json_file:
              json.dump(magicSplitList, json_file)

        # 아직 매수하지 않은 차수인 경우
        else:
          if purchasePlan['order'] > 1:
            previousPurchasePlan = getPurchasePlan(magicSplit['purchasePlans'], purchasePlan['order'] - 1)

            if previousPurchasePlan is not None and previousPurchasePlan['hasBought'] == True:
              previousProfitRate = (currentPrice - previousPurchasePlan['price']) / previousPurchasePlan['price'] * 100.0

              logger.info(f"{ticker} {purchasePlan['order']}차수 수익률: {round(previousProfitRate, 2)}% (트리거수익률: {selectedInvestmentPlan['triggerRate']}%)")

              additionalCondition = True

              # 홀수 차수인 경우
              if purchasePlan['order'] % 2 == 1:
                if yesterdayOpeningPrice < yesterdayClosingPrice and (yesterdayClosingPrice >= ma5Yesterday or ma5TwoDaysAgo <= ma5Yesterday):
                  additionalCondition = True
                else:
                  additionalCondition = False
              # 짝수 차수인 경우
              else:
                additionalCondition = True

              # 현재 손실률이 트리거 손실률보다 낮은 경우
              if previousProfitRate <= selectedInvestmentPlan['triggerRate'] and additionalCondition == True:
                currentBalance = upbit.get_balance(ticker)
                amountToBuy = selectedInvestmentPlan['amount']

                if amountToBuy < minBuyAmount:
                  amountToBuy = minBuyAmount

                # 시장가 매수
                balances = upbitTools.buyCoinOnMarketPrice(upbit, ticker, amountToBuy)

                purchasePlan['hasBought'] = True
                purchasePlan['price'] = currentPrice
                purchasePlan['amount'] = abs(upbit.get_balance(ticker) - currentBalance)

                with open(JSON_FILE_PATH, 'w') as json_file:
                  json.dump(magicSplitList, json_file)

                message = f"[{STRATEGY_NAME}] [{ticker}] {purchasePlan['order']}차수 매수 완료! 이전 차수 손실률: {selectedInvestmentPlan['triggerRate']}% 만족"
                logger.info(message)
                slack.sendMessage(message)

      isFullyBought = True

      for purchasePlan in magicSplit['purchasePlans']:
        if purchasePlan['hasBought'] == False:
          isFullyBought = False
          break

      if isFullyBought == True:
        lastInvestmentPlan = getInvestmentPlan(investmentPlans, int(installmentCount))
        lastPurchasePlan = getPurchasePlan(magicSplit['purchasePlans'], int(installmentCount))

        lastPurchaseProfitRate = (currentPrice - lastPurchasePlan['price']) / lastPurchasePlan['price'] * 100.0

        if (lastPurchaseProfitRate <= lastInvestmentPlan['triggerRate']):
          message = f"[{STRATEGY_NAME}] [{ticker}] 풀매수 상태인데 추가 하력하여 2차수 손절 및 초기화를 진행합니다!"
          logger.info(message)
          slack.sendMessage(message)

          secondPurchasePlan = getPurchasePlan(magicSplit['purchasePlans'], 2)

          amountToSell = secondPurchasePlan['amount']
          currentBalance = upbit.get_balance(ticker)

          isAmountToSellAdjusted = False
          if amountToSell > currentBalance:
            amountToSell = currentBalance
            isAmountToSellAdjusted = True

          # 시장가 매도
          balances = upbitTools.sellCoinOnMarketPrice(upbit, ticker, amountToSell)

          secondPurchasePlan['hasBought'] = False
          profit = calculateProfit(balances, ticker)
          magicSplit['realizedPNL'] += profit['amount'] * amountToSell / currentBalance

          message = f"[{STRATEGY_NAME}] [{ticker}] 2차수 손절 및 초기화 완료!"

          if isAmountToSellAdjusted == True:
            message = f"[{STRATEGY_NAME}] [{ticker}] 2차수 손절 및 초기화 완료! 매도할 수량이 보유 수량보다 많은 상태라 모두 매도함!"

          logger.info(message)
          slack.sendMessage(message)

          for i in range(int(installmentCount)):
            order = i + 1

            if order >= 2:
              purchasePlan = magicSplit['purchasePlans'][i]

              if order == int(installmentCount):
                purchasePlan['hasBought'] = False
                purchasePlan['price'] = 0
                purchasePlan['amount'] = 0

                message = f"[{STRATEGY_NAME}] [{ticker}] {order}차수 비워둠!\n {installmentCount}차수를 새로 매수할 수 있음!"
                logger.info(message)
                slack.sendMessage(message)

              else:
                purchasePlan['hasBought'] = magicSplit['purchasePlans'][i + 1]['hasBought']
                purchasePlan['price'] = magicSplit['purchasePlans'][i + 1]['price']
                purchasePlan['amount'] = magicSplit['purchasePlans'][i + 1]['amount']

                message = f"[{STRATEGY_NAME}] [{ticker}] {order + 1}차수 데이터를 {order}차수로 옮김!"
                logger.info(message)
                slack.sendMessage(message)

          with open(JSON_FILE_PATH, 'w') as json_file:
            json.dump(magicSplitList, json_file)

for magicSplit in magicSplitList:
  logger.info(f"[{STRATEGY_NAME}] [{magicSplit['ticker']}] 누적 실현 손익: {magicSplit['realizedPNL']}")
