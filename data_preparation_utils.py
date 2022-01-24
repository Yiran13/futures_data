from datetime import timedelta

import pandas as pd

from browning.future_kbar.future_kbar import get_future_kbar, get_raw_kbar
from browning.future_kbar.kbar_process.real_contracts import filter_real_contracts
from browning.future_kbar.kbar_process.used_name import intergrate_used_name_by_product


def generate_multiple_price(prod, begin_time, end_time, save_dir):
    # step1: 找到prod所有合约的日线close数据
    ahead_begin_time = begin_time - timedelta(days=5)
    contracts = intergrate_used_name_by_product(get_raw_kbar=get_raw_kbar, freq='1d', begin_time=ahead_begin_time,
                                                end_time=end_time, prod=prod)
    result = filter_real_contracts(contracts)
    all = result
    all.loc[:, 'date'] = all.index.strftime('%Y-%m-%d')
    all.loc[:, 'date_code'] = (all['code'].str[-4:]).astype('int')
    all.index = all.index + pd.Timedelta(hours=15)

    # step2： 生成contract_ref表记录每日的主力合约，carry合约，next主力合约
    daily_contracts_ref = find_daily_contracts_ref(all, begin_time, end_time, prod)

    # step3: 找到每日主力合约，carry合约，next主力合约的收盘价格
    filtered_all = all[['close', 'code', 'date']]
    daily_contracts_ref = find_contract_prices(daily_contracts_ref, filtered_all)

    # step4: 调整合约格式
    daily_contracts_ref = adjust_contracts_form(daily_contracts_ref, prod)

    # step5： 最后一次检查是否carry和主力存在一致的情况
    same_index = daily_contracts_ref.loc[
        daily_contracts_ref['CARRY_CONTRACT'] == daily_contracts_ref['PRICE_CONTRACT']].index.tolist()
    if len(same_index) > 0:
        print(f'{prod} 仍存在主力合约和carry合约一致的情况，需要检查')
        print(daily_contracts_ref.loc[same_index])
    else:
        # step13: 输出csv
        print(f'{prod} 生成multiple price 完毕')
        daily_contracts_ref.to_csv(f'{save_dir}/{prod}.csv', index=False)


def adjust_contracts_form(daily_contracts_ref, prod):
    """
    调整合约代码格式和列名
    """
    daily_contracts_ref.loc[:, 'current_contract'] = '20' + daily_contracts_ref['current_contract'].str.replace(
        f'{prod}', '') + '00'
    daily_contracts_ref.loc[:, 'next_contract'] = '20' + daily_contracts_ref['next_contract'].str.replace(f'{prod}',
                                                                                                          '') + '00'
    daily_contracts_ref.loc[:, 'carry_contract'] = '20' + daily_contracts_ref['carry_contract'].str.replace(f'{prod}',
                                                                                                            '') + '00'
    daily_contracts_ref.rename(columns={'carry_contract': 'CARRY_CONTRACT', 'next_contract': 'FORWARD_CONTRACT',
                                        'current_contract': 'PRICE_CONTRACT', 'date': 'DATETIME'}, inplace=True)
    daily_contracts_ref = daily_contracts_ref[
        ['DATETIME', 'PRICE', 'FORWARD', 'CARRY', 'PRICE_CONTRACT', 'FORWARD_CONTRACT', 'CARRY_CONTRACT']]
    daily_contracts_ref.loc[:, 'DATETIME'] = pd.to_datetime(daily_contracts_ref['DATETIME']) + pd.Timedelta(hours=15)
    return daily_contracts_ref


def find_contract_prices(daily_contracts_ref, filtered_all):
    """
    找到每日主力合约，carry合约，下个主力合约的价格
    """
    # step1: 找出carry合约的价格
    daily_contracts_ref = pd.merge(left=daily_contracts_ref, right=filtered_all, left_on=['carry_contract', 'date'],
                                   right_on=['code', 'date'])
    del daily_contracts_ref['code']
    daily_contracts_ref.rename(columns={'close': 'CARRY'}, inplace=True)

    # step2: 找出Forward的价格
    daily_contracts_ref = pd.merge(left=daily_contracts_ref, right=filtered_all, left_on=['next_contract', 'date'],
                                   right_on=['code', 'date'])
    del daily_contracts_ref['code']
    daily_contracts_ref.rename(columns={'close': 'FORWARD'}, inplace=True)

    # step5: 找出当前交易合约的价格
    daily_contracts_ref = pd.merge(left=daily_contracts_ref, right=filtered_all, left_on=['current_contract', 'date'],
                                   right_on=['code', 'date'])
    del daily_contracts_ref['code']
    daily_contracts_ref.rename(columns={'close': 'PRICE'}, inplace=True)
    return daily_contracts_ref


def find_daily_contracts_ref(all, begin_time, end_time, prod):
    """
    利用单个品种的所有合约close数据和主连k线生成一个dataframe，含有如下列：
    current_contract, carry_contract, next_contract

    """
    # step1: 找到当前交易的合约（主力）
    kbar = find_main_contract(begin_time, end_time, prod)
    current = kbar['trade_code']

    # step2：找到carry合约（这里选择近月）
    carry = all.groupby(all.index)['date_code'].min()

    # step3: 合并主力和carry合约
    daily_contracts_ref = pd.merge(left=carry, right=current, left_index=True, right_index=True)
    daily_contracts_ref.columns = ['carry_contract', 'current_contract']
    daily_contracts_ref.loc[:, 'carry_contract'] = daily_contracts_ref['carry_contract'].astype('str')
    daily_contracts_ref.loc[:, 'carry_contract'] = prod + daily_contracts_ref['carry_contract']

    # step5：针对近月和主力合约相同情况的修正
    same_index = daily_contracts_ref.loc[
        daily_contracts_ref['carry_contract'] == daily_contracts_ref['current_contract']].index.tolist()
    if len(same_index) > 0:
        slice_df = all.loc[same_index]
        slice_df.loc[:, 'date_rank'] = slice_df.groupby(level=0)['date_code'].rank()
        target_df = slice_df.loc[slice_df['date_rank'] == 2]
        daily_contracts_ref.loc[same_index, 'carry_contract'] = target_df['code'].values

    # step6:找到主力合约切换的日期,合并下一个主力合约（用于后续复权价格）,注意这里需要使用bfill
    roll_calendar = find_roll_date(kbar)
    next = roll_calendar['next_contract']
    daily_contracts_ref = pd.merge(left=daily_contracts_ref, right=next, left_index=True, right_index=True, how='outer')
    daily_contracts_ref.loc[:, 'next_contract'] = daily_contracts_ref['next_contract'].fillna(method='bfill')

    # step7: 针对当下，下个主力合约还没办法确定的情况，用当前主力合约替代（不会影响结果）
    fill_index = daily_contracts_ref.loc[daily_contracts_ref['next_contract'].isna()].index
    daily_contracts_ref.loc[fill_index, 'next_contract'] = daily_contracts_ref.loc[fill_index][
        'current_contract'].values

    daily_contracts_ref.loc[:, 'date'] = daily_contracts_ref.index.strftime('%Y-%m-%d')
    return daily_contracts_ref


def find_roll_date(kbar):
    """
    找到主力合约切换的时间点
    """
    roll_calendar = kbar.loc[kbar['trade_code'] != kbar['trade_code'].shift(-1)][['trade_code']]
    roll_calendar.columns = ['current_contract']
    roll_calendar.loc[:, 'next_contract'] = roll_calendar['current_contract'].shift(-1)
    roll_calendar.loc[:, 'date_code'] = (roll_calendar['current_contract'].str[-4:]).astype('int')
    roll_calendar.loc[:, 'date'] = roll_calendar.index.strftime('%Y-%m-%d')
    return roll_calendar


def find_main_contract(begin_time, end_time, prod):
    """
    使用主连30数据，找到每日的主力合约（从主连得到的主力合约，主力合约可能向前切换的问题）
    """
    kbar = get_future_kbar(code=f'{prod}30', freq='1d', begin_time=begin_time, end_time=end_time)
    kbar.loc[:, 'current_month'] = kbar.index.month
    exchange = kbar['exchange'].iloc[0]
    # 修改主连的trade_code,针对郑商所品种，修改为标准年月日
    if exchange == 'CZCE':
        month_code = '1' + kbar['trade_code'].str[-3:]
        kbar.loc[:, 'month_code'] = month_code

        refer_code = kbar['trade_day'].dt.year.astype('str').str[-2:] + kbar['trade_code'].str[-2:]
        kbar.loc[:, 'refer_code'] = refer_code

        kbar.loc[:, 'hold_month'] = kbar['trade_code'].str[-2:]

        correct_index = kbar.loc[kbar['refer_code'].astype('int') > kbar['month_code'].astype('int')].index
        kbar.loc[correct_index, 'month_code'] = '2' + kbar.loc[correct_index, 'month_code'].str[1:]
        kbar.loc[:, 'trade_code'] = prod + kbar['month_code']
    # 主力合约切换检查
    yesterday_contract = kbar.iloc[-2]['trade_code']
    today_contract = kbar.iloc[-1]['trade_code']
    if yesterday_contract != today_contract:
        print(f'注意 {prod}主力合约从{yesterday_contract}切换到{today_contract}')
    return kbar
