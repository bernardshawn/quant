import ccxt
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os

# 设置pandas显示选项
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# pd.set_option('display.precision', 4)

# 设定的季度合约日期
quarters = ['0328', '0627', '0927', '1227']
# 创建交易所对象
exchange = ccxt.binance({'proxies': {'http': '127.0.0.1:7890', 'https': '127.0.0.1:7890'}})

print("开始获取BN市场数据")

while True:
    try:
        # 获取数据并转换为DataFrame
        df_futures = exchange.fetch_tickers(params={'type': 'delivery'})

        df_futures = pd.DataFrame.from_dict(df_futures, orient='index')
        # 筛选包含特定季度信息的行
        df_futures = df_futures[df_futures['symbol'].apply(lambda x: any(q in x for q in quarters))]
        # 创建一个新的列来存储symbol值，假设索引已经是symbol
        df_futures = pd.DataFrame(df_futures['symbol'].values, columns=['symbol'])
        # print(df_futures)
        # exit()
        break
        # exit()
    except Exception as e:
        print(f"Error: {e}")


# 定义处理每个合约的函数
def process_contract(instrument):
    try:
        # instrument: BCH/USD:BCH-240628
        base_symbol_with_date = instrument.split(':')[1]  # BCH-240628
        base_symbol           = base_symbol_with_date.split('-')[0]  # BCH
        delivery_date_str     = base_symbol_with_date.split('-')[1]  # 240628
        future_symbol         = base_symbol + 'USD_' + delivery_date_str  # BCHUSD_240628 币本位交割
        perpetual_symbol      = base_symbol + 'USD_PERP'  # BCHUSD_PERP 币本位永续
        spot_symbol           = base_symbol + 'USDT'  # BCHUSDT 现货

        # 获取资金费率
        latest_funding_rate = exchange.dapiPublicGetPremiumIndex(params={'symbol': perpetual_symbol})
        latest_funding_rate = float(latest_funding_rate[0]['lastFundingRate'])
        # print(latest_funding_rate)
        # exit()

        avg_funding_rate_data = exchange.dapiPublicGetFundingRate(
            params={'symbol': perpetual_symbol, 'limit': 90})  # 资金费率历史条目数，最大1000
        avg_funding_rate_data = [float(record["fundingRate"]) for record in avg_funding_rate_data]
        avg_funding_rate = sum(avg_funding_rate_data) / len(avg_funding_rate_data) if avg_funding_rate_data else None
        # print('avg_funding_rate:', latest_funding_rate)
        # print("{:.3f}%".format(avg_funding_rate * 100))
        # exit()

        # 获取永续合约的价格 https://www.okx.com/docs-v5/zh/#order-book-trading-market-data-get-ticker
        perpetual_data  = exchange.dapiPublicGetTickerBookTicker(params={'symbol': perpetual_symbol})
        perpetual_price = float(perpetual_data[0]['bidPrice'])  # 买一价
        # print('perpetual_price:', perpetual_price)

        # 获取交割合约的价格
        delivery_data  = exchange.dapiPublicGetTickerBookTicker(params={'symbol': future_symbol})
        delivery_price = float(delivery_data[0]['askPrice'])  # 卖一价
        # print(delivery_price)

        # 获取现货价格
        spot_data  = exchange.publicGetTickerBookTicker(params={'symbol': spot_symbol})
        spot_price = float(spot_data['askPrice'])  # 卖一价 这个直接抓出来了字典，直接找键就行，和上面的不一样，上面抓出来的是列表

        # 计算价差和年化收益
        delivery_day = datetime.strptime(delivery_date_str, "%y%m%d")
        wait_day     = (delivery_day - datetime.now()).days + 1
        # print(wait_day)
        # 计算永续和交割的价差、年化收益
        perpetual_delivery_spread       = delivery_price - perpetual_price
        perpetual_delivery_spread_rate  = perpetual_delivery_spread / perpetual_price
        perpetual_delivery_annual_yield = (perpetual_delivery_spread / perpetual_price) * (365 / wait_day)
        perpetual_delivery_safe_yield   = perpetual_delivery_spread_rate / wait_day / 3

        # 计算现货和交割的价差、年化收益
        spot_delivery_spread       = delivery_price - spot_price
        spot_delivery_spread_rate  = spot_delivery_spread / spot_price
        spot_delivery_annual_yield = (spot_delivery_spread / spot_price) * (365 / wait_day)

        symbol_with_date = f"{base_symbol}-{delivery_date_str}"

        # 计算现货和交割的价差、年化收益

        return {
            '品种': symbol_with_date,
            '交割价格': delivery_price,
            '永续价格': perpetual_price,
            '交永价差': perpetual_delivery_spread,
            '交永价差%': perpetual_delivery_spread_rate,
            '交永价差年化%': perpetual_delivery_annual_yield,
            '交永保本费率%': perpetual_delivery_safe_yield,
            '14天资金费率平均值': avg_funding_rate,
            '最新资金费率': latest_funding_rate,
            '现货价格': spot_price,
            '交现价差': spot_delivery_spread,
            '交现价差%': spot_delivery_spread_rate,
            '交现价差年化%': spot_delivery_annual_yield,
        }

    except Exception as e:
        print(f"Error processing {instrument}: {e}")
        return None


# 使用ThreadPoolExecutor来并行处理合约
results = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(process_contract, instrument) for instrument in df_futures['symbol']]
    for future in as_completed(futures):
        result = future.result()
        if result:
            results.append(result)

# 使用pandas.concat代替append
result_list = pd.concat([pd.DataFrame([result]) for result in results], ignore_index=True)

result_list.set_index('品种', inplace=True)
result_list.sort_values(by='交永价差年化%', ascending=True, inplace=True)


# 最后两段代码统一做格式调整
def format_values(col_name, value):
    # 保留2位小数，并转换为百分比的列
    if col_name in ['交永价差%', '交永价差年化%', '交现价差%', '交现价差年化%']:
        return "{:.2f}%".format(value * 100)  # 转换为百分比并保留2位小数
    # 保留3位小数，并转换为百分比的列
    elif col_name in ['最新资金费率', '14天资金费率平均值', '交永保本费率%']:
        return "{:.3f}%".format(value * 100)  # 转换为百分比并保留3位小数
    else:
        return value  # 对于其他列，不做转换


# 遍历DataFrame的每一列，并应用格式转换
for col in result_list.columns:
    if col not in ['品种']:  # 排除"品种"列，因为它不需要转换
        result_list[col] = result_list[col].apply(lambda x: format_values(col, x))

print(result_list)

# 打印
# 获取当前登录用户的用户名并构建桌面路径
desktop_path     = os.path.join(os.path.expanduser("~"), 'Desktop')
output_file_path = os.path.join(desktop_path, 'BN费率.csv')

# 保存DataFrame到CSV，指定编码为utf-8-sig
result_list.to_csv(output_file_path, index=True, encoding='utf-8-sig')
print(f"文件已保存到：{output_file_path}")

# 假设result_list已经按照上述方式填充并排序

# 过滤出具有多个条目的唯一币种
unique_coins = result_list.index.to_series().apply(lambda x: x.split('-')[0]).unique()

# 准备一个DataFrame来存储结果
price_comparison_df = pd.DataFrame(columns=['交割合约价差'])

# 遍历唯一的币种
for coin in unique_coins:
    temp_df = result_list.loc[result_list.index.str.startswith(coin + '-')]
    # 确保数据按照交割日期排序
    temp_df = temp_df.sort_index()
    if temp_df.shape[0] > 1:
        # 使用排序后的日期，选取价格
        earlier_price = temp_df.iloc[0]['交割价格']
        later_price   = temp_df.iloc[1]['交割价格']
        price_ratio   = (later_price / earlier_price - 1) * 100
        # 存储结果
        price_comparison_df.loc[coin] = [f"{price_ratio:.2f}%"]

price_comparison_df.index.name = '品种'

print(price_comparison_df)
