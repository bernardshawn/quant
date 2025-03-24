import ccxt
import pandas as pd
import concurrent.futures
import multiprocessing
from tabulate import tabulate
import numpy as np

# 创建币安交易所实例
BINANCE_CONFIG = {
    'proxies': {'http': '127.0.0.1:7897', 'https': '127.0.0.1:7897'},
}
exchange = ccxt.binance(BINANCE_CONFIG)


# 获取交易信息
exchange_info = exchange.fapiPublicGetExchangeInfo()

# 提取所有交易对信息
symbols_info = exchange_info['symbols']

# 筛选符合条件的交易对
filtered_symbols = [info['symbol'] for info in symbols_info if
                    info['symbol'].endswith('USDT') and info['status'] == 'TRADING']

# 排除不需要的币种
exclude_symbols = ['XMRUSDT', 'USDCUSDT']
filtered_symbols = [symbol for symbol in filtered_symbols if symbol not in exclude_symbols]

# 转换为DataFrame
symbol_list = pd.DataFrame(filtered_symbols, columns=['symbol'])

# 报错的币种列表
error_symbols = []

# 获取CPU核心数
num_cores = multiprocessing.cpu_count()

def calculate_volatility(df):
    # 计算最近60天收盘价的标准差作为波动率
    return np.std(df['close'].iloc[-60:])

# 定义处理每个币种的函数
def process_symbol(row):
    symbol = row['symbol']
    max_retries = 5
    attempt = 0

    while attempt < max_retries:
        try:
            # 获取K线数据，获取尽可能多的历史数据
            kline_data = exchange.fapiPublicGetKlines({
                'symbol': symbol,
                'interval': '1d',
                'limit': 301  # 获取301天的K线数据
            })

            # 检查是否成功获取K线数据
            if not kline_data or len(kline_data) < 2:
                raise Exception(f"合约币种 {symbol} 无法获取足够的K线数据")

            # 转换为DataFrame
            df = pd.DataFrame(kline_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'trades', 'taker_base_volume', 'taker_quote_volume', 'ignore'])
            
            # 确保所有需要的列都转换为浮点数
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)

            # 计算均线
            ma5 = df['close'].rolling(window=5).mean().iloc[-1]
            ma20 = df['close'].rolling(window=20).mean().iloc[-1]
            ma60 = df['close'].rolling(window=60).mean().iloc[-1]
            latest_price = df['close'].iloc[-1]

            # 提取近300天的最高价
            highest_price_300_days = max(df['high'].iloc[:-1])

            # 获取最新价格和24小时数据
            ticker_data = exchange.fetch_ticker(symbol)

            # 检查是否成功获取ticker数据以及最新价格
            if not ticker_data or 'last' not in ticker_data:
                raise Exception(f"合约币种 {symbol} 无法获取最新价格")

            latest_price = ticker_data['last']
            price_change_percent = round(ticker_data.get('percentage', 0), 2)  # 24小时价格变动百分比，保留两位小数

            # 检查历史最高价和最新价格是否有效
            if highest_price_300_days is None or latest_price is None:
                raise Exception(f"数据不完整，{symbol} 的历史最高价或最新价格为空")

            # 计算下跌幅度并保留两位小数
            drop_ratio = round((1 - latest_price / highest_price_300_days) * 100, 2)

            # 计算波动率
            volatility = calculate_volatility(df)

            return symbol, highest_price_300_days, latest_price, drop_ratio, price_change_percent, ma5, ma20, ma60, volatility

        except Exception as e:
            print(f"合约币种 {symbol} 处理时出现异常: {e}")
            attempt += 1

    # 如果多次尝试后仍然失败，将该币种加入错误列表
    error_symbols.append(symbol)
    return None

# 使用线程池执行币种处理函数
with concurrent.futures.ThreadPoolExecutor(max_workers=num_cores) as executor:
    # 提交每个币种的处理任务
    task_list = [executor.submit(process_symbol, row) for index, row in symbol_list.iterrows()]

    # 获取结果
    results = [task.result() for task in concurrent.futures.as_completed(task_list) if task.result()]

# 存储符合条件的币种和对应的数据
price_data = [result for result in results if result is not None]

# 根据下跌幅度升序排列
price_data.sort(key=lambda x: x[3], reverse=False)

# 在price_data排序后添加
high_drop_symbols = [(symbol, drop_ratio) for symbol, _, _, drop_ratio, _, _, _, _, _ in price_data if drop_ratio > 90]

# 按下跌幅度从大到小排序
high_drop_symbols.sort(key=lambda x: x[1], reverse=True)

print("\n------------------------------------------------------")

# 准备表格数据
table_data = []
for index, (symbol, ath, latest_price, drop_ratio, price_change_percent, ma5, ma20, ma60, volatility) in enumerate(price_data, start=1):
    warning = "⚠️" if drop_ratio > 90 else ""
    table_data.append([
        index,  # 序号
        symbol,
        f"{ath:.8f}",
        f"{latest_price:.8f}",
        f"{drop_ratio}%",
        f"{price_change_percent}%",
        warning
    ])

# 打印表格
headers = ["序号", "币种", "历史最高价", "当前价格", "下跌幅度", "24小时变动", "警告"]
print(tabulate(table_data, headers=headers, tablefmt="pretty", numalign="decimal", stralign="center"))

# 单独打印下跌幅度大于90%的币种
if high_drop_symbols:
    print("\n⚠️ 下跌幅度大于90%的币种：")
    high_drop_table = [[index + 1, s, f"{d}%"] for index, (s, d) in enumerate(high_drop_symbols)]
    print(tabulate(high_drop_table, headers=["序号", "币种", "下跌幅度"], tablefmt="pretty", stralign="center"))

print(f"\n报错的币种列表({len(error_symbols)}):")
print(tabulate([[s] for s in error_symbols], headers=["币种"], tablefmt="pretty"))

# 查找多头排列的币种
bullish_data = [(symbol, latest_price, ma5, ma20, ma60) for symbol, _, latest_price, _, _, ma5, ma20, ma60, _ in price_data if latest_price >= ma5 >= ma20 >= ma60]

# 根据价格与MA5的百分比差异降序排列
bullish_data.sort(key=lambda x: ((x[1] - x[2]) / x[2]) * 100, reverse=True)

# 打印多头排列的币种
if bullish_data:
    print("\n多头排列的币种：")
    bullish_table = [[index + 1, symbol, f"{price:.8f}", f"{ma5:.8f}", f"{ma20:.8f}", f"{ma60:.8f}"] 
                     for index, (symbol, price, ma5, ma20, ma60) in enumerate(bullish_data)]
    print(tabulate(bullish_table, headers=["序号", "币种", "价格", "MA5", "MA20", "MA60"], tablefmt="pretty", stralign="center"))
else:
    print("没有找到多头排列的币种。")

# 查找波动率最低的10个币种
volatility_data = [(symbol, volatility) for symbol, _, _, _, _, _, _, _, volatility in price_data]
volatility_data.sort(key=lambda x: x[1])

# 打印波动率最低的10个币种
print("\n波动率最低的10个币种：")
low_volatility_table = [[index + 1, symbol, f"{volatility:.8f}"] for index, (symbol, volatility) in enumerate(volatility_data[:10])]
print(tabulate(low_volatility_table, headers=["序号", "币种", "波动率"], tablefmt="pretty", stralign="center"))
