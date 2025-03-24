# 导入各种库，必备
import pandas as pd
import ccxt
import time
from ccxt import delta
from datetime import timedelta

from requests import delete



#便于显示行列，必备
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)

#适合中国显示设置，必备
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


# 创建ccxt交易所
BINANCE_CONFIG = {
    'proxies': {'http': '127.0.0.1:7897', 'https': '127.0.0.1:7897'},
}
exchange = ccxt.binance(BINANCE_CONFIG) # 声明交易所
# 如果要两个所都加进来就改成binance_exchange,加上okx_exchange = ccxt.okx()  # 声明okx交易所
## 1、只查看交易所数据
# # 输入ccxt函数指令
# response = exchange.fapiPublicGetExchangeInfo()['symbols']
# info = pd.DataFrame(response)
# # print('获取交易规则和交易对\n', info, '\n')
# # exit()
# # binanceAPI文档：https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info

# 获取实时行情数据
'''
创建交易所后开始请求对应参数：k线数据
首先要设置参数:
名称	类型	是否必需	描述
symbol	STRING	YES	交易对
interval	ENUM	YES	时间间隔
startTime	LONG	NO	起始时间
endTime	LONG	NO	结束时间
limit	INT	NO	默认值:500 最大值:1500.

'''

params = {'symbol': 'BTCUSDT',
          'interval': '15m',
          'startTime':1737034200000,
          'endTime':1737123300000,
          'limit':100
# 如果limit小于100如50条，从开始时间算50条，如果大于100，只抽开始到结束的最多条（示例最多100条）
          }

kline_data = exchange.fapiPublicGetKlines(params=params)
kline_data = pd.DataFrame(kline_data)

kline_data.rename(columns={0:'开盘时间',1:'开盘价',2:'最高价',3:'最低价',4:'收盘价(当前K线未结束的即为最新价)',5:'成交量'
    ,6:'收盘时间',7:'成交额',8:'成交笔数',9:'主动买入成交量',10:'主动买入成交额',11:'请忽略该参数'}, inplace=True)
del kline_data['请忽略该参数']
# 调整时间参数
kline_data['开盘时间'] = pd.to_datetime(kline_data['开盘时间'],unit = 'ms')
kline_data['收盘时间'] = pd.to_datetime(kline_data['收盘时间'],unit = 'ms')
# 加八小时
kline_data[['开盘时间','收盘时间']] = kline_data[['开盘时间','收盘时间']] + timedelta(hours=8)   #两个[]代表选中两个column
# df['开盘时间','收盘时间'] = df['开盘时间','收盘时间'] + timedelta(hours=8)  # 北京时间
print(kline_data)
exit()

# d_获取账户信息、下单、撤单
# PZ9sfRYIRdER1ZDXUnFNtM9RRMgi0LwiiCdtBPkulgIYXsEtBEXl8z3N0GuF5vxX
# Bp3phuYqP2MIkQpex2KItPeSxZNF3Dc8vFs1DZmiNHer4eDcR952IUzVS2ry17kk

BINANCE_CONFIG = {
    'apiKey':'3Q7lGYXm1F1jr1fjxWEpM9nsaan4dd1kOqb9OOyo9GSpMgmUW1rINU3ONwKi0r0k',
    'secret':'4DDCF9frqaNiaAWS0a364LYfB9O9C0Ix5dGKzo5rfIp6OCFaWHUkosTicHmeIeRd',
    # 'rateLimit': 10,
    # 'verbose': False,
    # 'enableRateLimit': False,
    'proxies': {'http': '127.0.0.1:7897', 'https': '127.0.0.1:7897'},
}
exchange = ccxt.binance(BINANCE_CONFIG)

# 1.账户信息V2 (USER_DATA)
# params = {'timestamp': int(time.time() * 1000)}
# response = exchange.fapiPrivateV2GetAccount(params=params)['assets']
# account = pd.DataFrame(response)
# print('账户信息\n', account, '\n')

# 2.调整开仓杠杆 (TRADE)
'''
POST /fapi/v1/leverage
symbol	STRING	YES	交易对
leverage	INT	YES	目标杠杆倍数：1 到 125 整数
recvWindow	LONG	NO	
timestamp	LONG	YES	
'''
params = {'symbol':'BTCUSDT',
    'leverage':'10',
    'timestamp':int(time.time() * 1000)
}
leverage = exchange.fapiPrivatePostLeverage(params=params)
print('调整开仓杠杆\n', leverage, '\n')

# 3.下单 (TRADE)
# # POST /fapi/v1/order
'''
根据 order type的不同，某些参数强制要求，具体如下:

Type	强制要求的参数
LIMIT	timeInForce, quantity, price
MARKET	quantity
STOP, TAKE_PROFIT	quantity, price, stopPrice
STOP_MARKET, TAKE_PROFIT_MARKET	stopPrice
TRAILING_STOP_MARKET	callbackRate
'''

params = {
    'symbol':'BTCUSDT',
    'side':'BUY',
    'type':'limit',   #订单类型 LIMIT, MARKET, STOP, TAKE_PROFIT, STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET
    'timestamp':int(time.time() * 1000),
    'price': 100000,  # 下单价格,在限价单的时候启用，将type换成LIMIT
    'quantity': 0.002,  # 下单数量
    'timeInForce': 'GTC'  #订单类型https://www.hawkinsight.com/article/understanding-stock-order-time-in-force-decoding-gtc-ioc-fok-orders
}
response = exchange.fapiPrivatePostOrder(params=params)
order_info = pd.DataFrame(response,index=['symbol'])   #为什么加index？
order_id = order_info['orderId'].iloc[0]
print('下单\n', order_info, '\n')


# 4.查询订单 (USER_DATA)
# GET /fapi/v1/order
'''
symbol	STRING	YES	交易对
orderId	LONG	NO	系统订单号
origClientOrderId	STRING	NO	用户自定义的订单号
recvWindow	LONG	NO
timestamp	LONG	YES
'''
params = {
    'symbol':'BTCUSDT',
    'timestamp':int(time.time() * 1000),
    'orderId':order_id
}
response = exchange.fapiPrivateGetOrder(params=params)
order = pd.DataFrame(response,index=[0])
print('查询订单\n', order, '\n')
exit()

# 5.撤销订单 (TRADE)
# # DELETE /fapi/v1/order
# '''
# 名称	类型	是否必需	描述
# symbol	STRING	YES	交易对
# orderId	LONG	NO	系统订单号
# origClientOrderId	STRING	NO	用户自定义的订单号
# recvWindow	LONG	NO
# timestamp	LONG	YES
# orderId 与 origClientOrderId 必须至少发送一个
# '''
# params = {
#     'symbol':'BTCUSDT',
#     'timestamp':int(time.time() * 1000),
#     'orderId': order_id
# }
# response = exchange.fapiPrivateDeleteOrder(params=params)
# delete_order = pd.DataFrame(response,index=[0])
# print('撤销订单\n', delete, '\n')

# 补充：万向划转

# e_单个币种监测与下单

# symbol = 'BTCUSDT'  # 指定买入的币种
# quantity = 0.01  # 指定买入币种的数量
# price = 70000  # 指定买入币种的价格
#
# while True:
#     # 获取最新价格数据
#     data = exchange.fapiPublicGetTickerPrice(params={'symbol': symbol})   #括号里是什么意思？
#     new_price = float(data['price'])
#     print('最新价格：',new_price)
#     # 判断是否可交易
#     if new_price > price:
#         print('到达指定价，买入')
#         # 下单买入
#         params = {
#     'symbol':symbol,
#     'side':'BUY',
#     'type':'market',   #订单类型 LIMIT, MARKET, STOP, TAKE_PROFIT, STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET
#     'timestamp':int(time.time() * 1000),
#     'price': price,  # 下单价格,在限价单的时候启用，将type换成LIMIT
#     'quantity': quantity,  # 下单数量
#         }
#         break
#     else:
#         print('价格未小于指定价，5s后继续监测\n')
#         time.sleep(5)






