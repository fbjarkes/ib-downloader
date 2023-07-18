#!/usr/bin/env python
import argparse
from datetime import datetime, timedelta
import logging

from ib_insync import IB, Stock, util

logger = logging.getLogger(__name__)

FORMAT_TO_UTC_DATE = 2
BAR_SIZE_MAPPING = {
    '1min': '1 min',
    '5min': '5 mins',
    '15min': '15 mins',
    '30min': '30 mins',
    '60min': '1 hour',
    'day': '1 day',
    'week': '1W',
    'month': '1M'
}

def calculate_duration(days: int) -> str: 
    if days > 365:
        return f"{days // 365} Y"
    elif days > 30:
        return f"{days // 30} M"
    else:
        return f"{days} D"
    

def get_bars(symbol: str, ib: IB, duration: str, barSize: str):
    contract = Stock(symbol, 'SMART', 'USD')
    #contract = Stock('HM.B', 'SFB', 'SEK')
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr=duration,
        barSizeSetting=barSize, whatToShow='MIDPOINT', useRTH=True, formatDate=FORMAT_TO_UTC_DATE)

    df = util.df(bars)
    df.to_csv(f"{symbol}.csv")
    logger.info(f"Wrote {len(df)} lines to {symbol}.csv")

def download(symbols, file, timeframe, verbose, days, tz='America/New_York', id=0, host='127.0.0.1', port=7498):
    """
    TICKER # Stock type and SMART exchange

    TICKER-STK # Stock and SMART exchange

    TICKER-STK-EXCHANGE # Stock

    TICKER-STK-EXCHANGE-CURRENCY # Stock

    TICKER-CFD # CFD and SMART exchange

    TICKER-CFD-EXCHANGE # CFD

    TICKER-CDF-EXCHANGE-CURRENCY # Stock

    TICKER-IND-EXCHANGE # Index

    TICKER-IND-EXCHANGE-CURRENCY # Index

    TICKER-YYYYMM-EXCHANGE # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT # Future

    TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT # FOP

    TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # FOP

    TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP

    TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP

    CUR1.CUR2-CASH-IDEALPRO # Forex

    TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT # OPT

    TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # OPT

    TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT

    TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT
    """
    logger.info(f"Connecting IB {host}:{port} with id {id}")
    ib = IB()
    ib.connect(host, port, clientId=id, readonly=True, timeout=30)
    
    symbols = symbols.split(',')
    if file:
        with open(file) as f:
            symbols = [ticker.rstrip() for ticker in f.readlines() if not ticker.startswith('#')]
    
    duration = calculate_duration(int(days))

    for sym in symbols:
        get_bars(sym, ib, duration=duration, barSize=BAR_SIZE_MAPPING[timeframe])


def main():
    # set logging to debug
    logging.basicConfig(level=logging.INFO)
    #logging.getLogger('ib_insync').setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', default="SPY", help="Comma separated list of symbols", dest='symbols')
    parser.add_argument('--file', help='Read symbols from file', dest='file')
    parser.add_argument('--timeframe', default='day', dest='timeframe')
    parser.add_argument('-v', '--verbose', action='count', dest='verbose')
    parser.add_argument('--days', dest='days')
    parser.add_argument('--tz', default='America/New_York', dest='tz')
    parser.add_argument('--id', default='0', dest='id')
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=7498, type=int, dest='port')

    args = parser.parse_args()
    download(args.symbols, args.file, args.timeframe, args.verbose, args.days, args.tz, args.id, args.host, args.port)


if __name__ == '__main__':
    main()