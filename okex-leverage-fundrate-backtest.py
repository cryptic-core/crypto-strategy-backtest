# coding=utf-8
import asyncio
import aiohttp
import json
from datetime import datetime
import gspread
import dateutil.parser
import time
import codecs
import math
base_url = "https://aws.okex.com"
kline_req_url = base_url+"klines"
fundrate_req_url = base_url+"/api/swap/v3/instruments/"
itv='8h'

# google gspread
gc = gspread.service_account()
# owner
owner_email = 'jun.yeah7429@gmail.com'


# 初始本金
initfund = 100000
# 槓桿倍數
LEVERAGE = 4

async def request(session,url):
    headers = {'content-type': 'application/json'}
    async with session.get(url,headers=headers) as res:
        return await res.text()

async def fetch_instruments_okex():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        fundrate_url = base_url +'/api/swap/v3/instruments'
        #print(fundrate_url )
        retr = await request(session,fundrate_url)
        retarr = {}
        arrobj_r = json.loads(retr)
        for symb in arrobj_r:
            retarr[symb['underlying_index']] = symb['instrument_id']
        return retarr

async def collectdata_calc(instruments):    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        now = datetime.now()
        
        startTime = 1483228800000 # 2017/1/1
        endTime = int(time.mktime(now.timetuple())*1e3)
        retdict = {}
        
        for ins in instruments:
            retdict[ins] = {}
        for ins in instruments:
            t = startTime
            fundstart = '' # 真實的 fund start time
            fundend = '' # 真實的 fund end time
            compoundfund = initfund # 累計本金          
            positiveFundTimes = 0 # 勝率 資費為正的次數 
            totalFundTimes = 0  # 資費總次數
            
            
            # mdd
            hh = -9999
            dd = 9999
            mdd = 9999
            
            # 日內最大回撤
            prvdaynetprofit = -9999
            dmdd = 9999
            
            # 最長未創高區間
            lastHHTimestamp = 0
            longestHHPeriod = -9999
            
            
            # 波動率
            avgfundrate = 0
            prvcompfund = 0
            avgvolatility = 0
            fundratecoll = []
            
            fundrate_url = fundrate_req_url+ instruments[ins] +'/historical_funding_rate'
            #print(fundrate_url )
            retr = await request(session,fundrate_url)
            arrobj_r = json.loads(retr)
            arrobj_r = list(reversed(arrobj_r))
            for fds in arrobj_r:
                ktime = fds['funding_time']
                if(len(fundstart)<1):fundstart = ktime
                
                if(not ktime  in retdict[ins]):
                    fundrate = abs(float(fds['funding_rate'])) - 2.083e-05
                    if(fundrate>0):
                        positiveFundTimes +=1
                    totalFundTimes += 1
                    compoundfund += compoundfund*fundrate*LEVERAGE
                    
                    retdict[ins][ktime] = [fundrate,compoundfund-initfund]
                    fundratecoll.append(fundrate)
                    print(ins + ':' + str(ktime) + ' fundrate:'+str(fundrate)+' compoundfund:'+str(compoundfund))
                    
                    
                    
                    # 計算 d_dd 日內波動
                    if((totalFundTimes%3)==0):
                        print('one day has passed , current time = '+ str(t))
                        todaynetprofit = compoundfund*fundrate*(-1)*LEVERAGE
                        d_dd = todaynetprofit-prvdaynetprofit
                        if(d_dd < dmdd):dmdd = d_dd # d_dd
                        prvdaynetprofit = todaynetprofit
                    
                    # 計算 mdd 創高區間
                    if(compoundfund > hh):
                        hh=compoundfund
                        _kt = datetime.timestamp(dateutil.parser.parse(ktime))
                        if(lastHHTimestamp<1):
                            lastHHTimestamp = datetime.timestamp(dateutil.parser.parse(ktime))
                        period = (_kt - lastHHTimestamp)
                        if(period > longestHHPeriod):
                            longestHHPeriod = period
                        lastHHTimestamp = _kt
                    elif(compoundfund < hh):
                        dd = compoundfund - hh
                        if(dd < mdd):mdd=dd
                    
                    # 波動累加 報酬率累加
                    if(prvcompfund<1):prvcompfund = compoundfund
                    avgvolatility += abs(compoundfund - prvcompfund) / prvcompfund
                    avgfundrate += fundrate
                    prvcompfund = compoundfund
                    
                    fundend = ktime
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
            
            
            retdict[ins]['fundstart'] = fundstart
            retdict[ins]['fundend'] = fundend
            
            retdict[ins]['positiveFundTimes'] = positiveFundTimes
            retdict[ins]['totalFundTimes'] = totalFundTimes # 盈利次數
            
            
            retdict[ins]['compoundfund'] = compoundfund  # 總報酬
            retdict[ins]['winrate'] = winrate # 勝率
            retdict[ins]['longestHHPeriod'] = longestHHPeriod/86400000 # 創高區間
            retdict[ins]['mdd'] = (mdd/initfund)*100 # mdd
            retdict[ins]['dmdd'] = (dmdd/initfund)*100 # dmdd
            retdict[ins]['sharpe'] = sharpe # sharpe
            retdict[ins]['avgvolatility'] = avgvolatility # avgvolatility
            
            
        return retdict
    
async def backtest():
    
    
    file_object = codecs.open('fundrate_report.txt', 'w', "utf-8")
    file_object.write('')
    file_object.close()
    
    instruments = await fetch_instruments_okex()
    fundhist = await collectdata_calc(instruments)
    
    for ins in instruments:
        starttime = fundhist[ins]['fundstart']
        endtime = fundhist[ins]['fundend'] 
        starttime_dt = datetime.timestamp(dateutil.parser.parse(starttime))
        endtime_dt = datetime.timestamp(dateutil.parser.parse(endtime))
        
        durationDays = abs(starttime_dt - endtime_dt) / 86400
        onedayret = (fundhist[ins]['compoundfund'] - initfund)/durationDays/initfund
        yearret = onedayret * 365 * 100
        yearret_str = "{:.2f}".format(yearret)
        
        positiveRatio = (fundhist[ins]['positiveFundTimes'] / fundhist[ins]['totalFundTimes'])*100
        positiveRatio_str = "{:.2f}".format(positiveRatio)
        
        netReturn = fundhist[ins]['compoundfund'] - initfund
        netReturn_str = "{:.2f}".format(netReturn)
        grossRate = (netReturn / initfund)/durationDays*365*100
        grossRate_str = "{:.2f}".format(grossRate)
        
        msg = u''
        msg += '##  **'+ins + '** \n'
        msg += u'回測時間:'+ starttime +' to '+ endtime+ ' 共 ' +str(int(durationDays)) + ' 天 \n\n'
        msg += u'初始資金:'+ str(initfund) +' USD\n\n'
        msg += u'費率為正次數:'+ str(fundhist[ins]['positiveFundTimes'])+'\n\n'
        msg += u'總領費率次數:'+ str(fundhist[ins]['totalFundTimes'])+'\n\n'
        msg += u'勝率:'+ positiveRatio_str +'%\n\n'
        msg += u'總利潤:'+ netReturn_str +'USD\n\n'
        msg += u'最大創高區間:'+ ("{:.2f}".format(fundhist[ins]['longestHHPeriod'])) +'天\n\n'
        msg += u'最大拉回:'+ ("{:.2f}".format(fundhist[ins]['mdd'])) +'%\n\n'
        msg += u'每日最大拉回:'+ ("{:.2f}".format(fundhist[ins]['dmdd'])) +'%\n\n'
        msg += u'夏普比率:'+ ("{:.2f}".format(fundhist[ins]['sharpe'])) +'\n\n'
        msg += u'波動率:'+ ("{:.2f}".format(fundhist[ins]['avgvolatility'])) +'%\n\n'
        msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n\n'
        msg += u'年化報酬:'+ grossRate_str +'%\n\n'
        
        
        # 圖表資料 直接上傳到 google spreadsheet
        sheetID = ''
        rowstart = 2
        rowend = 2
        rows = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            kline_url = fundrate_req_url+ instruments[ins] +'/candles?start='+ starttime +'&end='+ endtime+'&granularity=14400'
            ret = await request(session,kline_url)
            arrobj = json.loads(ret)
            if(len(arrobj)<1):return
            
        
            # combine data
            timestampcol = []
            timeprice = {}
            prvrate = 0
            prvnetprofit = 0
            prvprice = 0  
            
            sh = gc.create('okex-'+ ins)
            sh.share(None, perm_type='anyone', role='reader')
            sheetID = sh.id
            worksheet = sh.get_worksheet(0)
            worksheet.update('A1:D1',[['time','fundrate','netprofit','price']])
            
            
            for key in fundhist[ins]:
                if('Z' in key):
                    closesttimek = {}
                    tdiff = 9999
                    _fundt = datetime.timestamp(dateutil.parser.parse(key))
                    _rtimespl1 = key.split('T')
                    _rtimespl2 = _rtimespl1[1].split('.')
                    _rtime = _rtimespl1[0] + ' ' + _rtimespl2[0]
                    for kline in arrobj:
                        _kt = datetime.timestamp(dateutil.parser.parse(kline[0]))
                        _diff = abs(_fundt - _kt)
                        if(_diff < tdiff):
                            closesttimek = kline
                        if(_diff<1):break
                    # 注意除了第一欄之外其餘都要是浮點數
                    rows.append([_rtime, fundhist[ins][key][0]*3*365 , fundhist[ins][key][1] ,float(closesttimek[4])])
                    rowend += 1
                    
            worksheet.update('A'+ str(rowstart)+':D'+str(rowend),rows)
            time.sleep(1)
        notionchart = 'https://notion.vip/notion-chart/draw.html?config_documentId='
        notionchart += sheetID
        notionchart += '&config_sheetName=Sheet1'
        notionchart += '&config_dataRange=A2%3AD'+str(rowend)
        notionchart += '&config_chartType=line&config_theme=lightMode&option_hAxis_format=MM%2Fdd%2FYY&option_vAxis_format=%23%2C%23%23%23&option_colors=%23D9730D%2C%230B6E99%2C%237D7C78'
        
        gdrivelink = '[明細](https://docs.google.com/spreadsheets/d/'+sheetID+'/edit?usp=sharing)'

        
        msg += '\n\n'
        msg += '**'+ins+'**\n\n'
        msg += '[績效圖]('+notionchart+')\n\n'
        msg += gdrivelink + '\n\n'
        msg += '---\n\n'
        
        # 寫入 md
        file_object = codecs.open('fundrate_report.md', 'a', "utf-8")
        file_object.write(msg)
        file_object.close()
    
    # --------------------------------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------------------------------
    
    # 綜合績效
    # 目前人工挑選，長遠來看具備上漲基本面及
    # 及歷史回測績效較好的幣種
    coinlist = ['SNX','IOTA','MATIC','RVN','SNX']
    coinweight = {'SNX':0.18,'IOTA':0.35,'MATIC':0.24,'RVN':0.1,'SNX':0.13}
    
    
    yearret = 0
    positiveRatio = 0
    mdd = 9999
    dmdd = 9999
    sharpe = 0
    avgvolatility = 0
    netReturn = 0
    grossRate = 0
    
    starttime = fundhist['IOTA']['fundstart']
    endtime = fundhist['IOTA']['fundend']    
    starttime_dt = datetime.timestamp(dateutil.parser.parse(starttime))
    endtime_dt = datetime.timestamp(dateutil.parser.parse(endtime)) 
    durationDays = abs(starttime_dt - endtime_dt) / 86400    

    positiveFundTimes = 0
    totalFundTimes = 0
    for ins in coinlist:
        
        onedayret = (fundhist[ins]['compoundfund'] - initfund)/durationDays/initfund
        yearret += onedayret * 365 * 100 * coinweight[ins]
        positiveFundTimes += fundhist[ins]['positiveFundTimes']
        totalFundTimes += fundhist[ins]['totalFundTimes']        
        positiveRatio += (fundhist[ins]['positiveFundTimes'] / fundhist[ins]['totalFundTimes']) * 100 * coinweight[ins]
        netReturn += (fundhist[ins]['compoundfund'] - initfund) * coinweight[ins]
        grossRate += ((netReturn / initfund)/durationDays*365*100) * coinweight[ins]
        sharpe += fundhist[ins]['sharpe'] * coinweight[ins]
        avgvolatility += fundhist[ins]['avgvolatility'] * coinweight[ins]
        if(fundhist[ins]['mdd']<mdd ):mdd=fundhist[ins]['mdd']
        if(fundhist[ins]['dmdd']<dmdd ):dmdd=fundhist[ins]['dmdd']
    
    
    msg = u''
    msg += "**'SNX','IOTA','MATIC','RVN','SNX' 綜合費率套利績效**\n\n"
    msg += u'回測時間:'+str(starttime_dt)+' to '+str(endtime_dt)+ ' 共 ' +str(int(durationDays)) + ' 天 \n\n'
    msg += u'初始資金:'+ str(initfund) +' USD\n\n'
    msg += u'費率為正次數:'+ str(positiveFundTimes)+'\n\n'
    msg += u'總領費率次數:'+ str(totalFundTimes)+'\n\n'
    msg += u'勝率:'+  ("{:.2f}".format(positiveRatio)) +'%\n\n'
    msg += u'總利潤:'+ ("{:.2f}".format(netReturn)) +'USD\n\n'
    msg += u'最大拉回:'+ ("{:.2f}".format(mdd)) +'%\n\n'
    msg += u'每日最大拉回:'+ ("{:.2f}".format(dmdd)) +'%\n\n'
    msg += u'夏普比率:'+ ("{:.2f}".format(sharpe)) +'\n\n'
    msg += u'波動率:'+ ("{:.2f}".format(avgvolatility)) +'%\n\n'
    msg += u'每月報酬:'+ ("{:.2f}".format(grossRate/12.0)) +'%\n\n'
    msg += u'年化報酬:'+ ("{:.2f}".format(grossRate)) +'%\n\n'
    
    file_object = codecs.open('fundrate_report.md', 'a', "utf-8")
    file_object.write(msg)
    file_object.close()
    
    timestampcol = []
    for ins in coinlist:
        for key in fundhist[ins]:
            if('Z' in key):
                timestampcol.append(key)
    
    timestampcol.sort()
    
    sh = gc.create('okex-combine-report')
    sh.share(None, perm_type='anyone', role='reader')
    sheetID = sh.id
    worksheet = sh.get_worksheet(0)
    worksheet.update('A1:C1',[['time','fundrate','netprofit']])
    rowstart = 2
    rowend = 2
    rows = []    
    prvrate = {}
    prvnetprofit = {}
    for tt in timestampcol:
        for ins in coinlist:
            if(tt in fundhist[ins]):
                prvrate[ins] = (fundhist[ins][tt][0] * 3 * 365 * 100) * coinweight[ins]
                prvnetprofit[ins] = (fundhist[ins][tt][1]) * coinweight[ins]
        
        def _sum(arr):
            sum=0
            for i in arr:
                sum = sum + i
            return(sum)               
        rate = _sum( list(prvrate.values()) )
        netprofit = _sum( list(prvnetprofit.values()) )
        
        # convert time format
        _fundt = datetime.timestamp(dateutil.parser.parse(tt))
        _rtimespl1 = tt.split('T')
        _rtimespl2 = _rtimespl1[1].split('.')
        _rtime = _rtimespl1[0] + ' ' + _rtimespl2[0]            
        rows.append([_rtime, rate , netprofit ])
        rowend +=1
    
    worksheet.update('A'+ str(rowstart)+':D'+str(rowend),rows)
    notionchart = 'https://notion.vip/notion-chart/draw.html?config_documentId='
    notionchart += sheetID
    notionchart += '&config_sheetName=Sheet1'
    notionchart += '&config_dataRange=A2%3AD'+str(rowend)
    notionchart += '&config_chartType=line&config_theme=lightMode&option_hAxis_format=MM%2Fdd%2FYY&option_vAxis_format=%23%2C%23%23%23&option_colors=%23D9730D%2C%230B6E99%2C%237D7C78'
    
    gdrivelink = '[明細](https://docs.google.com/spreadsheets/d/'+sheetID+'/edit?usp=sharing)'
    msg += '\n\n'
    msg += " **'SNX','IOTA','MATIC','RVN','SNX'**\n\n"
    msg += '[績效圖]('+notionchart+')\n\n'
    msg += gdrivelink + '\n\n'
    msg += '---\n\n'
    
    # 寫入 md
    file_object = codecs.open('fundrate_report.md', 'a', "utf-8")
    file_object.write(msg)
    file_object.close()
    
    print('done')

if __name__ == "__main__":
    asyncio.run(backtest())
