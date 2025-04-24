# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import dateutil.parser
import time
import csv
import codecs
import math
base_url = "https://api.binance.com/api/v3/"
base_url_fapi = "https://fapi.binance.com"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url_fapi+"/fapi/v1/fundingRate"

base_url_huo = "https://api.hbdm.com"
fundrate_req_url_huo = "/linear-swap-api/v1/swap_historical_funding_rate"

base_url_ok = "https://aws.okex.com"
fundrate_req_url_ok = "/api/swap/v3/instruments/" #"/historical_funding_rate"



itv='8h'

# 初始本金
initfund = 100000


async def request(session,url):
    headers = {'content-type': 'application/json'}
    async with session.get(url,headers=headers) as res:
        return await res.text()

async def fetch_instruments_binance():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        fundrate_url = base_url_fapi+'/fapi/v1/exchangeInfo'
        #print(fundrate_url )
        retr = await request(session,fundrate_url)
        arrobj_r = json.loads(retr)
        retarr = {}
        for symb in arrobj_r['symbols']:
            if(symb['contractType'] == 'PERPETUAL'):
                retarr[symb['baseAsset']] = (symb['symbol'])
        return retarr
        
async def fetch_instruments_huobi():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        fundrate_url = base_url_huo+'/linear-swap-api/v1/swap_open_interest'
        #print(fundrate_url )
        retr = await request(session,fundrate_url)
        retarr = {}
        arrobj_r = json.loads(retr)
        for symb in arrobj_r['data']:
            retarr[symb['symbol']] = symb['contract_code']
        return retarr

async def fetch_instruments_okex():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        fundrate_url = base_url_ok+'/api/swap/v3/instruments'
        #print(fundrate_url )
        retr = await request(session,fundrate_url)
        retarr = {}
        arrobj_r = json.loads(retr)
        for symb in arrobj_r:
            retarr[symb['underlying_index']] = symb['instrument_id']
        return retarr


async def fetch_binance_rate_history(symbols,alltime,starttimef):
    bSymbRTimeseries = {}
    seriestimecol = []
    for symb in symbols:
        timeratedict = {}
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            now = datetime.now()
            startTime = 1483228800000 # 2017/1/1
            t = startTime
            endTime = int(time.mktime(now.timetuple())*1e3)
            
            while(t <= endTime):
                fundrate_url = fundrate_req_url +'?symbol='+ symbols[symb] +'&startTime='+str(t)+'&limit=1000'
                #print(fundrate_url )
                retr = await request(session,fundrate_url)
                arrobj_r = json.loads(retr)
                
                for fds in arrobj_r:
                    ktime = int(fds['fundingTime']/1000)
                    if(not ktime  in timeratedict):
                        fundrate = float(fds['fundingRate'])
                        timeratedict[ktime] = fundrate
                        if(not ktime in alltime): #
                            alltime.append(ktime)
                            print('binance',ktime)
                    if(not ktime  in timeratedict):
                        seriestimecol.append(ktime)
                    
                    if(not ktime  in seriestimecol):
                        seriestimecol.append(ktime)                    
                if(len(arrobj_r)<1):
                    break
                t = arrobj_r[len(arrobj_r)-1]['fundingTime']+1000 # 拿最後一筆資料的收盘时间當作下一個的開頭
        bSymbRTimeseries[symb] = timeratedict
    seriestimecol.sort()
    seriesstart = seriestimecol[0]
    if(seriesstart>starttimef):starttimef = seriesstart
    return bSymbRTimeseries,starttimef

async def fetch_huobi_rate_history(symbols,alltime,starttimef):
    hSymbRTimeseries = {}
    seriestimecol = []
    for symb in symbols:
        timeratedict = {}
        totalpage = 0
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            fundrate_url = base_url_huo + fundrate_req_url_huo +'?contract_code='+ symbols[symb]
            retr = await request(session,fundrate_url)
            arrobj_r = json.loads(retr)
            totalpage = arrobj_r['data']['total_page']
            if(totalpage>0):
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session2:
                    fundrate_url_t = base_url_huo + fundrate_req_url_huo +'?contract_code='+ symbols[symb]+'&page_size='+str(totalpage)
                    print(fundrate_url_t)
                    retr2 = await request(session2,fundrate_url_t)
                    arrobj_r2 = json.loads(retr2)
                    
                    for fds in arrobj_r2['data']['data']:
                        ktime = int(int(fds['funding_time'])/1000)
                        if(not ktime  in timeratedict):
                            fundrate = float(fds['realized_rate'])
                            timeratedict[ktime] = fundrate
                            if(not ktime in alltime): #
                                alltime.append(ktime)
                            else:                                
                                print('huobi has funding time',ktime)
                        
                        if(not ktime  in seriestimecol):
                            seriestimecol.append(ktime)
                        if(len(arrobj_r)<1):
                            break
            
        hSymbRTimeseries[symb] = timeratedict
    seriestimecol.sort()
    seriesstart = seriestimecol[0]
    if(seriesstart>starttimef):starttimef = seriesstart
    return hSymbRTimeseries,starttimef

async def fetch_okex_rate_history(symbols,alltime,starttimef):
    hSymbRTimeseries = {}
    startfime = 0
    seriestimecol = []   
    for symb in symbols:
        timeratedict = {}
        totalpage = 0
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            fundrate_url = base_url_ok + fundrate_req_url_ok + symbols[symb] +"/historical_funding_rate"
            print(fundrate_url)
            retr = await request(session,fundrate_url)
            arrobj_r = json.loads(retr)
            
            for fds in arrobj_r:
                ktime = int(time.mktime(dateutil.parser.parse(fds['funding_time']).timetuple()))
                if(startfime <1):startfime = ktime
                if(not ktime  in timeratedict):
                    fundrate = float(fds['realized_rate'])
                    timeratedict[ktime] = fundrate
                    if(not ktime in alltime): #
                        alltime.append(ktime)
                    else:
                        print('okex has funding time',ktime)
                if(not ktime  in seriestimecol):
                    seriestimecol.append(ktime)
                
            if(len(arrobj_r)<1):
                break
            
        hSymbRTimeseries[symb] = timeratedict
    seriestimecol.sort()
    seriesstart = seriestimecol[0]
    if(seriesstart>starttimef):starttimef = seriesstart
    return hSymbRTimeseries,starttimef



def aggregate(alltime,bin_series,huo_series,ok_series,starttimef):
    # collect all timestamp
    retdict = {}
    
    compoundfund = initfund # 累計本金          
    positiveFundTimes = 0 # 勝率 資費為正的次數 
    totalFundTimes = 0  # 資費總次數
    
    
    # mdd
    hh = -9999
    dd = 0
    mdd = 0
    
    # 日內最大回撤
    prvdaynetprofit = -9999
    dmdd = 0
    
    # 最長未創高區間
    lastHHTimestamp = 0
    longestHHPeriod = -9999
    
    
    # 波動率
    avgfundrate = 0
    prvcompfund = 0
    avgvolatility = 0
    fundratecoll = []
    
    timestart = 0
    
    def checkdif(ex1,ex2,ser1,ser2,name1,name2,feedif=-9999):
        override = False
        avaliable1 = curtime in ser1[name1]
        avaliable2 = curtime in ser2[name2]
        if(avaliable1 and avaliable2):
            
            fee1 = ser1[name1][curtime]
            fee2 = ser2[name2][curtime]
            print(ex1,ex2,curtime,name1,fee1,fee2)
            negative = (fee1 * fee2) < 0
            if(negative):
                _feedif = abs(fee1)+abs(fee2)
                if(_feedif > feedif):
                    feedif = _feedif
                    override = True
        return feedif,override
    startIdx = alltime.index(starttimef)
    for k in range(startIdx,len(alltime)):
        curtime = alltime[k]
        feediff = -9999
        ex1=''
        ex2=''
        coinN=''
        for coinA in bin_series:
            for coinB in huo_series:
                for coinC in ok_series:
                    if(coinA==coinB):
                        #print('now ',curtime,' check binance & huobi', coinA )
                        feediff,override = checkdif('binance','huobi',bin_series, huo_series,coinA,coinB,feediff)
                        if(override):
                            ex1='binance'
                            ex2='houbi'
                            coinN=coinA
                    if(coinB==coinC):
                        #print('now ',curtime,' check huobi & okex', coinC )
                        feediff,override = checkdif('huobi','okex',huo_series, ok_series,coinB,coinC,feediff)
                        if(override):
                            ex1='houbi'
                            ex2='okex'
                            coinN=coinB
                    if(coinC==coinA):
                        #print('now ',curtime,' check huobi & okex', coinA )
                        feediff,override = checkdif('okex','binance', ok_series, bin_series,coinC,coinA,feediff)
                        if(override):
                            ex1='okex'
                            ex2='binance'
                            coinN=coinA                        
        if(not curtime  in retdict):
            if(feediff>0):
                if(timestart<1):timestart = curtime
                totalFundTimes += 1
                positiveFundTimes +=1            
                compoundfund += compoundfund*feediff
            
                retdict[curtime] = [feediff,compoundfund-initfund,ex1,ex2,coinN]
                fundratecoll.append(feediff)
                print(str(curtime) + ' fundrate:'+str(feediff)+' compoundfund:'+str(compoundfund))
                avgfundrate += feediff
                
            
            # 計算 d_dd 日內波動
            if((totalFundTimes%3)==0):
                #print('one day has passed , current time = '+ str(curtime))
                todaynetprofit = compoundfund*feediff
                d_dd = todaynetprofit-prvdaynetprofit
                if(d_dd < dmdd):dmdd = d_dd # d_dd
                prvdaynetprofit = todaynetprofit
            
            # 計算 mdd 創高區間
            if(compoundfund > hh):
                hh=compoundfund
                if(lastHHTimestamp<1):lastHHTimestamp=curtime
                period = (curtime - lastHHTimestamp)
                if(period > longestHHPeriod):
                    longestHHPeriod = period
                lastHHTimestamp = curtime
            elif(compoundfund < hh):
                dd = compoundfund - hh
                if(dd < mdd):mdd=dd
            
            # 波動累加 報酬率累加
            if(prvcompfund<1):prvcompfund = compoundfund
            avgvolatility += abs(compoundfund - prvcompfund) / prvcompfund
            
            prvcompfund = compoundfund
            
    # 計算績效
    avgfundrate /= totalFundTimes
    avgvolatility /= totalFundTimes
    winrate = (positiveFundTimes / totalFundTimes)*100
    
    # sharp
    def variance(data, ddof=0):
        n = len(data)
        mean = sum(data) / n
        return sum((x - mean) ** 2 for x in data) / (n - ddof)
    def stdev(data):
        var = variance(data)
        std_dev = math.sqrt(var)
        return std_dev            
    sharpe = avgfundrate / stdev(fundratecoll)
    
    
    retdict['fundstart'] = starttimef
    retdict['fundend'] = alltime[-1]
    
    retdict['positiveFundTimes'] = positiveFundTimes
    retdict['totalFundTimes'] = totalFundTimes # 盈利次數
    
    
    retdict['compoundfund'] = compoundfund  # 總報酬
    retdict['winrate'] = winrate # 勝率
    retdict['longestHHPeriod'] = longestHHPeriod/86400000 # 創高區間
    retdict['mdd'] = (mdd/initfund)*100 # mdd
    retdict['dmdd'] = (dmdd/initfund)*100 # dmdd
    retdict['sharpe'] = sharpe # sharpe
    retdict['avgvolatility'] = avgvolatility # avgvolatility
    
    
    return retdict
    
async def backtest():
    alltime = []
    binance_instruments = await fetch_instruments_binance()
    huobi_instruments = await fetch_instruments_huobi()
    okex_instruments = await fetch_instruments_okex()
    starttimef = 0
    binance_symb_rate_timeseries,starttimef = await fetch_binance_rate_history(binance_instruments,alltime,starttimef)
    huobi_symb_rate_timeseries,starttimef = await fetch_huobi_rate_history(huobi_instruments,alltime,starttimef)
    okex_symb_rate_timeseries,starttimef = await fetch_okex_rate_history(okex_instruments,alltime,starttimef)
    alltime.sort()
    fundhist = aggregate(alltime,binance_symb_rate_timeseries,huobi_symb_rate_timeseries,okex_symb_rate_timeseries,starttimef)
    
    file_object = codecs.open('fundrate_report.txt', 'w', "utf-8")
    file_object.write('')
    file_object.close()
    
    starttime = fundhist['fundstart']
    endtime = fundhist['fundend']
    starttime_dt = datetime.fromtimestamp(starttime)
    endtime_dt = datetime.fromtimestamp(endtime)
    
    durationDays = (fundhist['fundend'] - fundhist['fundstart']) / 86400
    onedayret = (fundhist['compoundfund'] - initfund)/durationDays/initfund
    yearret = onedayret * 365 * 100
    yearret_str = "{:.2f}".format(yearret)
    
    positiveRatio = (fundhist['positiveFundTimes'] / fundhist['totalFundTimes'])*100
    positiveRatio_str = "{:.2f}".format(positiveRatio)
    
    netReturn = fundhist['compoundfund'] - initfund
    netReturn_str = "{:.2f}".format(netReturn)
    grossRate = (netReturn / initfund)/durationDays*365*100
    grossRate_str = "{:.2f}".format(grossRate)
    
    
    msg = u''
    msg += 'cross exchange (binance,huobi,okex) fundingrate arbitrage backtest \n'
    msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n'
    msg += u'初始資金:'+ str(initfund) +'USD\n'
    msg += u'費率為正次數:'+ str(fundhist['positiveFundTimes'])+'\n'
    msg += u'總領費率次數:'+ str(fundhist['totalFundTimes'])+'\n'
    msg += u'勝率:'+ positiveRatio_str +'%\n'
    msg += u'總利潤:'+ netReturn_str +'USD\n'
    msg += u'最大創高區間:'+ ("{:.2f}".format(fundhist['longestHHPeriod'])) +'天\n'
    msg += u'最大拉回:'+ ("{:.2f}".format(fundhist['mdd'])) +'%\n'
    msg += u'每日最大拉回:'+ ("{:.2f}".format(fundhist['dmdd'])) +'%\n'
    msg += u'夏普比率:'+ ("{:.2f}".format(fundhist['sharpe'])) +'\n'
    msg += u'波動率:'+ ("{:.2f}".format(fundhist['avgvolatility'])) +'%\n'
    msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n'
    msg += u'年化報酬:'+ grossRate_str +'%\n\n'
    
    
    file_object = codecs.open('fundrate_report.txt', 'a', "utf-8")
    file_object.write(msg)
    file_object.close()        
    
    
    def intTryParse(value):
        val = None
        try:val = int(value)
        except:pass
        if(val!=None):return True
        return False
        
    with open( ('aggregate_price.csv'), mode='w') as fprice_file:
        fprice_file = csv.writer(fprice_file , delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fprice_file.writerow(['time','fundrate','netprofit','exchangeA','exchangeB','coinName'])
        for tt in fundhist:
            if(intTryParse(tt)):
                fprice_file.writerow([tt,fundhist[tt][0] ,fundhist[tt][1],fundhist[tt][2],fundhist[tt][3],fundhist[tt][4]])
    
    print('done')

if __name__ == "__main__":
    asyncio.run(backtest())
