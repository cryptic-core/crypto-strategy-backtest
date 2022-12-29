# coding=utf-8
import sys,os
import asyncio
import aiohttp
import json
from datetime import datetime
import pandas as pd
import time
import csv
import codecs
import math
import csv
import random
import string
import numpy as np
import quantstats as qs


base_url = "https://api.binance.com/api/v3/"
kline_req_url = base_url+"klines"
itv='1h'
instruments=['ETH']

ONE_DAY = 86400
now = int(datetime.now().timestamp())
two_month_ago = now - ONE_DAY * 60

def mean(data):
    n = len(data)
    _mean = sum(data) / n    
    return _mean

def variance(data, ddof=0):
    n = len(data)
    mean = sum(data) / n
    return sum((x - mean) ** 2 for x in data) / (n - ddof)
def stdev(data):
    var = variance(data)
    std_dev = math.sqrt(var)
    return std_dev
# 布林帶標準算式
def calc_entry_price_boll(klines,cnt,boll_cnt,boll_std):
    closearr = []
    for k in range(len(klines)):
        if(k%cnt == 0):
            closearr.append(float(klines[-k-1][1]))
        if(len(closearr)>=boll_cnt):
            break
    mean = sum(closearr) / len(closearr)
    std = stdev(closearr)
    upper = mean + std * boll_std
    lower = mean - std * boll_std
    return upper,lower

def calcLiquidity(
    cprice,
    upper,
    lower,
    amountX,
    amountY
):
    liquidity = 0
    if(cprice <= lower):
        liquidity = amountX * (math.sqrt(upper) * math.sqrt(lower)) / (math.sqrt(upper) - math.sqrt(lower))
    elif(upper < cprice):
        Lx = amountX * (math.sqrt(upper) * math.sqrt(cprice)) / (math.sqrt(upper) - math.sqrt(cprice))
        Ly = amountY / (math.sqrt(cprice) - math.sqrt(lower))
        liquidity = min(Lx,Ly)
    else:
        liquidity = amountY / (math.sqrt(upper) - math.sqrt(lower))
    
    return liquidity


def getTokenAmountsFromDepositAmounts(
    P,  # 初始建倉位置
    Pl, # 下界 Pl = lower
    Pu, # 上界 Pu = upper
    priceUSDX, # 幣種1對美元 priceUSDX = cprice_eth
    priceUSDY, # 幣種2對美元 priceUSDY = 1
    targetAmounts # 投入資金美金計價 targetAmounts = initCapital
):
    deltaL = targetAmounts / ((math.sqrt(P) - math.sqrt(Pl)) * priceUSDY + 
                            (priceUSDX / math.sqrt(P) - priceUSDX / math.sqrt(Pu)) )

    deltaY = deltaL * (math.sqrt(P) - math.sqrt(Pl))
    if (deltaY * priceUSDY < 0):
        deltaY = 0
    if (deltaY * priceUSDY > targetAmounts): 
        deltaY = targetAmounts / priceUSDY

    deltaX = deltaL * (1 / math.sqrt(P) - 1 / math.sqrt(Pu))
    if (deltaX * priceUSDX < 0):
        deltaX = 0;
    if (deltaX * priceUSDX > targetAmounts):
        deltaX = targetAmounts / priceUSDX

    return deltaX,deltaY


def getILPriceChange(
    price, # 初始建倉位置
    newPrice, # 當前幣價 P=cprice_eth
    upper,  # 上界 Pu = upper
    lower, # 下界 Pl = lower
    amountX, # 初始eth數量
    amountY # 初始u數量
):
    L = calcLiquidity(price,upper,lower,amountX,amountY)
    deltaP = math.sqrt(newPrice) - math.sqrt(price)
    oneOverDeltaP = 1/(math.sqrt(newPrice)) - 1/(math.sqrt(price))
    deltaX = oneOverDeltaP * L
    deltaY = deltaP * L
    Lx2 = max(amountX + deltaX,0)
    Ly2 = max(amountY + deltaY,0)
    #newAssetValue = Lx2 * newPrice + Ly2
    
    return Lx2,Ly2


def hourRangeCompute(ins): # ins = instruments[0]
    
    ONE_DAY = 86400-1
    all_klines = []
    with open(os.getcwd()+'/data/'+ins+'_1h.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    # 日統計
    atr_day = 0
    atr_day_std = 0
    daycnt = 0
    lastrecord_day = 1402942400
    range_day_hist_max = -99999999 # 歷史上曾發生過的最大真實區間 day
    
    
    europe_start = 16
    america_start = 21
    
    
    atr_days = {'asia':[],'europe':[],'america':[]}
    hour_atr = {}
    for h in range(24):
        hour_atr[h] = []
    
    skip_num = 24*365*2 # 前兩年不看
    # 計算hr震幅平均 & std
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        kl_open = float(kl[1])
        kl_close = float(kl[4])
        _rng = abs(kl_open-kl_close)/kl_open*100
        hour_atr[curhour].append(_rng)
        
        
        #dt = datetime.fromtimestamp(kl[0])
        #print(_rng)
    
    hrmax = []
    hravg = []
    for o in range(24):
        hrmax.append(max(hour_atr[o]))
        hravg.append(mean(hour_atr[o]))
    print(hrmax)
    print(max(hrmax)) # 凌晨1點波動最大
    print(hrmax.index(min(hrmax))) # 早上10點平均最小
    print(hravg.index(max(hravg))) # 凌晨 12 點平均最大
    print(hravg.index(min(hravg))) # 早上9點平均最小
    print('end')


def backtest_neutral_low_vol(ins):
    ONE_DAY = 86400-1
    all_klines = []
    with open(os.getcwd()+'/data/'+ins+'_1h.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    
    start_hour = 6
    end_hour = 16
    perc = 0.09
    initCapital = 100000
    punish_cnt = 0
    punishment = 0.005
    total_cnt = 0
    entry_price_upper = 0
    entry_price_lower = 0
    
    skip_num = 24*365*2 # 前兩年不看
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        if(curhour == start_hour):
            kl_open = float(kl[1])
            entry_price_upper = kl_open*(1+perc)
            entry_price_lower = kl_open*(1-perc)
            total_cnt += 1
        if(curhour == end_hour) and (entry_price_upper != 0):
            kl_close = float(kl[4])
            if(kl_close > entry_price_upper):
                punish_cnt +=1
                initCapital *= 1-punishment
            if(kl_close < entry_price_lower):
                punish_cnt +=1
                initCapital *= 1-punishment
            entry_price_upper = 0
            entry_price_lower = 0        
        #dt = datetime.fromtimestamp(kl[0])
        #print(_rng)
    
    print('final capital '+str(initCapital) + ' failed cnt '+str(punish_cnt) + ' total cnt '+str(total_cnt))



# 計算布林值，上中下軌
# boll
boll_cnt = 20 
boll_std = 2
C24 = 1.01
C25 = 1
C26 = 0.95
def calc_price_ranking(klines):
    
    curprice = float(klines[-1][1])
    C5 = curprice
    cnt_day = int(24)
    cnt_4hr = int(4)
    cnt_1hr = 1
    #cnt_15m = 15
    upper_day,lower_day = calc_entry_price_boll(klines,cnt_day,boll_cnt,boll_std)
    upper_4hr,lower_4hr = calc_entry_price_boll(klines,cnt_4hr,boll_cnt,boll_std)
    upper_1hr,lower_1hr = calc_entry_price_boll(klines,cnt_1hr,boll_cnt,boll_std)
    #upper_15m,lower_15m = calc_entry_price_boll(klines,cnt_15m,boll_cnt,boll_std)
    
    E5 = upper_day
    C30 = E5
    C32 = upper_4hr
    C34 = upper_1hr
    C36 = upper_1hr
    D31 = (C30+C32)/2
    D33 = (C32+C34)/2
    D35 = (C34+C36)/2
    E32 = (D31+D33)/2
    E34 = (D33+D35)/2
    F33 = (E32+E34)/2
    
    
    H5 = lower_day
    H17 = upper_1hr
    
    C39 = H5
    C41 = lower_4hr
    C43 = lower_1hr
    C45 = lower_1hr
    D40 = (C39+C41)/2
    D42 = (C41+C43)/2
    D44 = (C43+C45)/2
    E41 = (D40+D42)/2
    E43 = (D42+D44)/2
    F42 = (E41+E43)/2
    
    # 短頂
    E23 = (F33+C5)/2*C24
    # 短底
    E27 = (F42+C5)/2*C26
    # 中
    E25 = (E23+E27)/2
    # (中＋頂) / 2
    E24 = (E23+E25)/2
    # (中＋底) / 2
    E26 = (E25+E27)/2
    
    return {'HH':E23,'HM':E24,'MM':E25,'ML':E26,'LL':E27,
            'BDU':upper_day,'BDL':lower_day,
            'B4U':upper_4hr,'B4L':lower_4hr,
            'B1U':upper_1hr,'B1L':lower_1hr,
            #'B15U':upper_15m,'B15L':lower_15m,
            }

def backtest_boll_entry_long(ins):
    print('start boll_entry_long test')
    ONE_DAY = 86400-1
    all_klines = []
    with open(os.getcwd()+'/data/'+ins+'_1h.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    
    initCapital = 100000
    entry_price_upper = 0
    entry_price_lower = 0
    entry_time = 0
    punish_cnt = 0
    punishment = 0.8
    riskratio = 0.03
    start_hour = 6
    priceRank = {}
    #skip_num = 24*365*2 # 前兩年不看
    skip_num = 24*365
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        numbar = min(boll_cnt*24,k)
        if(curhour == start_hour):
            priceRank = calc_price_ranking(all_klines[k-numbar:k+1])
            #print('new price rank setup ' + str(priceRank['LL']))
        
        if(priceRank != {}):
            if(entry_price_upper<0.001):
                #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
                if(float(kl[3])<=priceRank['LL']):
                    entry_price_upper = priceRank['HH']
                    entry_price_lower = priceRank['LL']*0.5
                    entry_time = k
                    print('new entry price' + str(entry_price_lower) + ' entry time ' + str(kl[0]))
            
            
            if(k>entry_time):
                # gain money
                if(entry_price_lower>0):
                    if(float(kl[2])>=entry_price_upper):
                        print( str(kl[2]) + ' win , hit price upper ' + str(entry_price_lower) + 'exit time' +str(kl[0]) ) 
                        initCapital /= punishment
                        entry_price_upper = 0
                        entry_price_lower = 0
            
                # lose money
                if(float(kl[3])<=entry_price_lower):
                    print( str(kl[3]) + ' lost , hit stoploss ' + str(entry_price_lower) + '@' +str(kl[0]) ) 
                    initCapital *= punishment
                    punish_cnt+=1
                    entry_price_upper = 0
                    entry_price_lower = 0
            
            
            
            
    print('final capital '+str(initCapital) + ' failed cnt '+str(punish_cnt))

def backtest_boll_entry_short(ins):
    print('start boll entry short test')
    all_klines = []
    with open(os.getcwd()+'/data/'+ins+'_1h.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    
    initCapital = 100000
    entry_price_upper = 0
    entry_price_lower = 0
    entry_time = 0
    punish_cnt = 0
    punishment = 0.8
    riskratio = 0.03
    start_hour = 6
    priceRank = {}
    #skip_num = 24*365*2 # 前兩年不看
    skip_num = 24*365
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        numbar = min(boll_cnt*24,k)
        if(curhour == start_hour):
            priceRank = calc_price_ranking(all_klines[k-numbar:k+1])
            #print('new price rank setup ' + str(priceRank['LL']))
        
        if(priceRank != {}):
            if(entry_price_lower<0.001):
                #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
                if(float(kl[2])>=priceRank['HH']):
                    hedgeamt = initCapital*0.7
                    short_coin_amt = hedgeamt / float(kl[2])
                    liquidation_price = initCapital / short_coin_amt 
                    entry_price_upper = liquidation_price*0.9 
                    entry_price_lower = priceRank['LL']
                    entry_time = k
                    print('new short entry :' + kl[2] +' sl:'+ str(entry_price_upper) + ' (before liquidation) entry time ' + str(kl[0]))
            
            
            if(k>entry_time):
                # gain money
                if(entry_price_lower>0):
                    if(float(kl[3])<=entry_price_lower):
                        print( str(kl[3]) + ' win , hit price lower ' + str(entry_price_lower) + 'exit time' +str(kl[0]) ) 
                        initCapital /= punishment
                        entry_price_upper = 99999999
                        entry_price_lower = 0
            
                # lose money
                if(float(kl[3])>=entry_price_upper):
                    print( str(kl[3]) + ' lost , hit stoploss ' + str(entry_price_upper) + '@' +str(kl[0]) )
                    initCapital *= punishment
                    punish_cnt+=1
                    entry_price_upper = 99999999
                    entry_price_lower = 0
            
            
            
            
    print('short test final capital '+str(initCapital) + ' failed cnt '+str(punish_cnt))


def backtest_longshort_IL_change(ins):
    print('start boll dual IL Change entry test')
    all_klines = []
    infile = os.getcwd()+'/data/'+ins+'_1h.csv'
    with open(infile) as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    # prepare dataframe for qs
    # 每日統計
    dateparse = lambda x:pd.to_datetime(x).date()  # datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(infile, parse_dates=['Open time'], date_parser=dateparse, encoding='utf-8')
    dft = df.drop_duplicates(subset=['Open time'],keep='last')
    dt_ret = dft.filter(['Open time','Close'], axis=1)
    #dt_ret['return'] = dt_ret['Close'].astype(float).pct_change(1)
    #dt_ret.set_index(pd.DatetimeIndex(dt_ret['Open time']),inplace=True)
    qsdf = dt_ret[['Open time','Close']].copy()
    
    
    
    initCapital = 100000
    equity_long = initCapital * 0.5
    entry_price_upper_long = 0
    entry_price_lower_long = 0
    entry_price_long = 0
    entry_amt0_long = 0
    entry_amt1_long = 0
    deltaX_long=0
    deltaY_long=0
    
    equity_short = initCapital * 0.5
    entry_price_upper_short = 0
    entry_price_lower_short = 0
    entry_price_short = 0
    entry_amt0_short = 0
    entry_amt1_short = 0
    hedgeRatio = 0.9 # 做空比率
    miningRatio = 0.9 # 拿來做 uniswap LP 比率，增加部位中性程度
    longAmt=0
    shortAmt=0
    deltaX_short=0
    deltaY_short=0
    
    entry_time = 0
    punish_cnt = 0
    # records
    priceRank = {}
    start_hour = 22 # record 專用
    #skip_num = 24*365*2 # 前兩年不看
    skip_num = 24*365
    qsdf = qsdf.iloc[int(skip_num/24):]
    qsdf.reset_index(inplace=True)
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        numbar = min(boll_cnt*24,k)
        if(curhour == start_hour): # collect record
            priceRank = calc_price_ranking(all_klines[k-numbar:k+1])
            #print('new price rank setup ' + str(priceRank['LL']))
            net_value = equity_long + equity_short
            
            #_timstamp = datetime.strptime(kl[0].split(' ')[0],'%Y-%m-%d').timestamp()
            #_ts = qsdf['Open time'].iloc[k-skip_num]
            #qsdf['Open time'].iloc[0] 
            qsdf.at[k-skip_num,'Close'] = net_value
            #print(qsdf.at[_dt, 'Close'])
            
        if(priceRank != {}):
            
            if(entry_price_lower_short<0.001):
                #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
                # short entry
                if(float(kl[2])>=priceRank['HH']):
                    hedgeamt = equity_short*0.7
                    short_coin_amt = hedgeamt / float(kl[2])
                    liquidation_price = equity_short / short_coin_amt 
                    entry_price_upper_short = liquidation_price*0.9
                    entry_price_lower_short = priceRank['LL']
                    
                    # 
                    entry_price_short = priceRank['HH']
                    # 一開始借幣數量
                    hedgeUSDAmt = equity_short * hedgeRatio
                    # 借來的顆數
                    shortAmt = hedgeUSDAmt/entry_price_short
                    # 借來的顆數實際拿下去作市的數量
                    miningAmt = shortAmt*miningRatio
                    # 借來的，持幣不參與作市
                    longAmt = shortAmt - miningAmt 
                    mining_usd_amt = hedgeUSDAmt
                    deltaX_short,deltaY_short = getTokenAmountsFromDepositAmounts(entry_price_short, entry_price_lower_short, entry_price_upper_short, entry_price_short, 1, mining_usd_amt)
                    
                    entry_time = k
                    print('------- new short entry :' + kl[2] +' sl:'+ str(entry_price_upper_short)  +' fund:'+ str(equity_short) + ' entry time ' + str(kl[0]))
            
            # long entry
            #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
            if(entry_price_lower_long<0.001):
                if(float(kl[3])<=priceRank['LL']):
                    entry_price_upper_long = priceRank['HH']
                    entry_price_lower_long = priceRank['LL'] * 0.2
                    entry_price_long = priceRank['LL']
                    entry_time = k
                    deltaX_long,deltaY_long = getTokenAmountsFromDepositAmounts(entry_price_long, entry_price_lower_long, entry_price_upper_long, entry_price_long, 1, equity_long)
                    
                    print('+++++++ new long entry price '+ kl[2] + ' sl:'+ str(entry_price_lower_long)  +' fund:'+ str(equity_long) + ' entry time ' + str(kl[0]))
        
        
            if(k>entry_time):
                # short win 
                if(entry_price_lower_short>0):
                    if(float(kl[3])<=entry_price_lower_short):
                        
                        P = entry_price_lower_short
                        # 當前經過 IL 計算之後部位剩餘顆數
                        P_clamp = min(max(P,entry_price_lower_short),entry_price_upper_short)
                        amt1,amt2 = getILPriceChange(entry_price_short,P_clamp,entry_price_upper_short,entry_price_lower_short,deltaX_short,deltaY_short)
                        curamt = amt1+amt2/P + longAmt
                        equity_short = equity_short - (shortAmt - curamt)*P
                        print(  '￥ short win , hit price lower ' + str(entry_price_lower_short) + ' lower price:'+str(kl[3]) +' fund:'+ str(equity_short) +' exit time' +str(kl[0]) )
                        
                        entry_price_upper_short = 99999999
                        entry_price_lower_short = 0
            
                    # short lost
                    if(float(kl[2])>=entry_price_upper_short):
                        P = entry_price_upper_short
                        # 當前經過 IL 計算之後部位剩餘顆數
                        P_clamp = min(max(P,entry_price_lower_short),entry_price_upper_short)
                        amt1,amt2 = getILPriceChange(entry_price_short,P_clamp,entry_price_upper_short,entry_price_lower_short,deltaX_short,deltaY_short)
                        curamt = amt1+amt2/P + longAmt
                        equity_short = equity_short - (shortAmt - curamt)*P
                        print( 'xxx            short lost , hit stoploss ' + str(entry_price_upper_short) + ' curr price:'+ str(kl[2])  +' fund:'+ str(equity_short) + '@' +str(kl[0]) )
                        punish_cnt+=1
                        entry_price_upper_short = 99999999
                        entry_price_lower_short = 0
                

                # long win
                if(entry_price_lower_long>0):
                    if(float(kl[2])>=entry_price_upper_long):
                        P = entry_price_upper_long
                        # 當前經過 IL 計算之後部位剩餘顆數
                        P_clamp = min(max(P,entry_price_lower_long),entry_price_upper_long)
                        amt1,amt2 = getILPriceChange(entry_price_long,P_clamp,entry_price_upper_long,entry_price_lower_long,deltaX_long,deltaY_long)
                        equity_long = amt1 * P + amt2
                        print( ' ￥ long win , hit price upper ' + str(entry_price_upper_long) + ' curr high:'+str(kl[2]) +' fund:'+ str(equity_long) + ' exit time' +str(kl[0]) ) 
                        
                        entry_price_upper_long = 0
                        entry_price_lower_long = 0
            
                # long lost
                if(float(kl[3])<=entry_price_lower_long):
                    P = entry_price_lower_long
                    # 當前經過 IL 計算之後部位剩餘顆數
                    P_clamp = min(max(P,entry_price_lower_long),entry_price_upper_long)
                    amt1,amt2 = getILPriceChange(entry_price_long,P_clamp,entry_price_upper_long,entry_price_lower_long,deltaX_long,deltaY_long)
                    initCapital = amt1 * P + amt2
                    print( 'xxx         long lost , hit stoploss ' + str(entry_price_lower_long) + ' curr low:'+str(kl[3]) +' fund:'+ str(initCapital) +  '@' +str(kl[0]) )
                    
                    punish_cnt+=1
                    entry_price_upper_long = 0
                    entry_price_lower_long = 0        
    
    # report 
    qsdf['return'] = qsdf['Close'].astype(float).pct_change(1)
    #matx = qs.reports.metrics(qsdf['return'],benchmark='Close' ,mode='full',display=True,prepare_returns=True)
    htmls = qs.reports.html(qsdf['return'],benchmark='Close',title="uni-aave-longshort",output="uni-aave-longshort.html")
    
    print('dual test final capital '+str( equity_long + equity_short ) + ' failed cnt '+str(punish_cnt))

def backtest_boll_longshort(ins):
    print('start boll dual entry test')
    all_klines = []
    with open(os.getcwd()+'/data/'+ins+'_1h.csv') as file_name:
        file_read = csv.reader(file_name)
        all_klines = list(file_read)
    all_klines.pop(0)
    
    
    initCapital = 100000
    entry_price_upper_long = 0
    entry_price_lower_long = 0
    entry_price_long = 0
    
    entry_price_upper_short = 0
    entry_price_lower_short = 0
    entry_price_short = 0
    
    entry_time = 0
    punish_cnt = 0
    punishment = 0.8
    riskratio = 0.03
    start_hour = 6
    priceRank = {}
    #skip_num = 24*365*2 # 前兩年不看
    skip_num = 24*365
    for k in range(skip_num,len(all_klines)-1):
        kl = all_klines[k]
        curhour = int(kl[0].split(' ')[1].split(':')[0])
        numbar = min(boll_cnt*24,k)
        if(curhour == start_hour):
            priceRank = calc_price_ranking(all_klines[k-numbar:k+1])
            #print('new price rank setup ' + str(priceRank['LL']))
        
        if(priceRank != {}):
            
            if(entry_price_lower_short<0.001):
                #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
                # short entry
                if(float(kl[2])>=priceRank['HH']):
                    hedgeamt = initCapital*0.7
                    short_coin_amt = hedgeamt / float(kl[2])
                    liquidation_price = initCapital / short_coin_amt 
                    entry_price_upper_short = liquidation_price*0.9 
                    entry_price_lower_short = priceRank['LL']
                    entry_price_short = priceRank['HH']
                    entry_time = k
                    print('------- new short entry :' + kl[2] +' sl:'+ str(entry_price_upper_short) + ' (before liquidation) entry time ' + str(kl[0]))
            
                # long entry
                #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
            if(entry_price_lower_long<0.001):
                if(float(kl[3])<=priceRank['LL']):
                    entry_price_upper_long = priceRank['HH']
                    entry_price_lower_long = priceRank['LL'] * 0.5
                    entry_price_long = priceRank['LL']
                    entry_time = k
                    print('+++++++ new long entry price '+ kl[2] + ' sl:'+ str(entry_price_lower_long) + ' entry time ' + str(kl[0]))
        
        
            if(k>entry_time):
                # gain money
                if(entry_price_lower_short>0):
                    if(float(kl[3])<=entry_price_lower_short):
                        print(  '￥ short win , hit price lower ' + str(entry_price_lower_short) + ' lower price:'+str(kl[3]) + ' exit time' +str(kl[0]) ) 
                        loss_perc = (entry_price_lower_short - entry_price_short)
                        initCapital /= punishment
                        entry_price_upper_short = 99999999
                        entry_price_lower_short = 0
            
                # lose money
                if(float(kl[2])>=entry_price_upper_short):
                    print( 'xxx            short lost , hit stoploss ' + str(entry_price_upper_short) + ' curr price:'+ str(kl[2]) + '@' +str(kl[0]) )
                    initCapital *= punishment
                    punish_cnt+=1
                    entry_price_upper_short = 99999999
                    entry_price_lower_short = 0
            
                    if(priceRank != {}):
                        if(entry_price_upper_short<0.001):
                            #print('price test '+str(float(kl[3])) + ' ' + str(priceRank['ML']))
                            if(float(kl[3])<=priceRank['LL']):
                                entry_price_upper_short = priceRank['HH']
                                entry_price_lower_short = priceRank['LL']*0.5
                                entry_time = k
                                print(' short new entry price' + str(entry_price_lower_short) + ' entry time ' + str(kl[0]))
                        
            
                # gain money
                if(entry_price_lower_long>0):
                    if(float(kl[2])>=entry_price_upper_long):
                        print( ' ￥ long win , hit price upper ' + str(entry_price_upper_long) + ' curr high:'+str(kl[2]) + ' exit time' +str(kl[0]) ) 
                        initCapital /= punishment
                        entry_price_upper_long = 0
                        entry_price_lower_long = 0
            
                # lose money
                if(float(kl[3])<=entry_price_lower_long):
                    print( 'xxx         long lost , hit stoploss ' + str(entry_price_lower_long) + ' curr low:'+str(kl[3]) + '@' +str(kl[0]) )
                    initCapital *= punishment
                    punish_cnt+=1
                    entry_price_upper_long = 0
                    entry_price_lower_long = 0        

            
    print('dual test final capital '+str(initCapital) + ' failed cnt '+str(punish_cnt))

for ins in instruments:
    hourRangeCompute(ins)
    backtest_neutral_low_vol(ins)
    #backtest_boll_entry_long(ins)
    #backtest_boll_entry_short(ins)
    #backtest_boll_longshort(ins)
    backtest_longshort_IL_change(ins)
    
print('finished')