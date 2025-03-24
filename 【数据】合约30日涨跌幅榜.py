import ccxt
import pandas as pd
import concurrent.futures
import multiprocessing

# 创建币安交易所实例
BINANCE_CONFIG = {
    'proxies': {'http': '127.0.0.1:7897', 'https': '127.0.0.1:7897'},
}
exchange = ccxt.binance(BINANCE_CONFIG)

# 排除指定的币种
exclude_symbols = ['XMRUSDT', 'USDCUSDT']

# 获取交易信息
exchange_info = exchange.fapiPublicGetExchangeInfo()

# 提取所有交易对信息
symbols_info = exchange_info['symbols']

# 筛选符合条件的交易对
filtered_symbols = [info['symbol'] for info in symbols_info if
                    info['symbol'].endswith('USDT') and info['status'] == 'TRADING']

filtered_symbols = [symbol for symbol in filtered_symbols if symbol not in exclude_symbols]

# 转换为DataFrame
symbol_list = pd.DataFrame(filtered_symbols, columns=['symbol'])

# 打印结果以确认symbol_list正确
# print(symbol_list.head())

# 报错的币种列表
error_symbols = []

# 获取CPU核心数
num_cores = multiprocessing.cpu_count()


# 定义处理每个币种的函数
def process_symbol(row):
    symbol = row['symbol']
    max_retries = 5
    attempt = 0

    while attempt < max_retries:
        try:
            # 获取K线数据，最近31天的K线数据
            kline_data = exchange.fapiPublicGetKlines({
                'symbol': symbol,
                'interval': '1d',
                'limit': 31  # 获取最近31天的K线数据
            })

            # 检查是否成功获取K线数据
            if not kline_data or len(kline_data) == 0:
                raise Exception(f"合约币种 {symbol} 无法获取K线数据")

            # 排除最新的一天数据，剩下前29天数据
            kline_data = kline_data[:-1]

            # 提取前29天内的最低价
            lows_29 = min(float(data[3]) for data in kline_data)

            # 获取最新价格
            ticker_data = exchange.fetch_ticker(symbol)

            # 检查是否成功获取ticker数据以及最新价格
            if not ticker_data or 'last' not in ticker_data:
                raise Exception(f"合约币种 {symbol} 无法获取最新价格")

            latest_price = ticker_data['last']

            # 检查最低价和最新价格是否有效
            if lows_29 is None or latest_price is None:
                raise Exception(f"数据不完整，{symbol} 的最低价或最新价格为空")

            # 打印调试信息
            print(f"{symbol}: 前30天内最低价: {lows_29}, 最新价格: {latest_price}")

            # 计算价格比例并保留两位小数
            ratio_29 = round((latest_price / lows_29 - 1) * 100, 2)

            return symbol, ratio_29  # 返回币种和对应的价格比例

        except Exception as e:
            print(f"合约币种 {symbol} 处理时出现异常: {e}")
            attempt += 1
            # sleep(2)  # 等待2秒后重试，可以根据需求启用

    # 如果多次尝试后仍然失败，将该币种加入错误列表
    error_symbols.append(symbol)
    return None


# 使用线程池执行币种处理函数
with concurrent.futures.ThreadPoolExecutor(max_workers=num_cores) as executor:
    # 提交每个币种的处理任务
    task_list = [executor.submit(process_symbol, row) for index, row in symbol_list.iterrows()]

    # 获取结果
    results = [task.result() for task in concurrent.futures.as_completed(task_list) if task.result()]

# 存储符合条件的币种和对应的价格比例
price_ratios = [result for result in results if result is not None]

# 根据最近30天价格比例降序排列
price_ratios.sort(key=lambda x: x[1], reverse=True)

print("\n------------------------------------------------------")

# 打印近30天涨幅最快前5名
print("\n30天爬升最快前10名：")
for symbol, ratio_29 in price_ratios[:10]:
    print(f"{symbol}: {ratio_29}%")

# 打印近30天涨幅最慢的前5名（升序排列）
print("\n30天爬升最慢前10名：")
for symbol, ratio_29 in price_ratios[-10:]:
    print(f"{symbol}: {ratio_29}%")

print(f"\n报错的币种列表({len(error_symbols)}):")
print(error_symbols)
