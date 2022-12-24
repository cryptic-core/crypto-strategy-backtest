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

base_url = "https://api.binance.com/api/v3/"
kline_req_url = base_url+"klines"
itv='1h'
instruments=['MATIC']

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


#    
for ins in instruments:
    hourRangeCompute(ins)
    backtest_neutral_low_vol(ins)
    #backtest_boll_entry_long(ins)
    backtest_boll_entry_short(ins)

print('finished')