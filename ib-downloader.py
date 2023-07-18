#!/usr/bin/env python
import argparse
from datetime import datetime, timedelta
import logging

from ib_insync import IB, Stock, util

logger = logging.getLogger(__name__)

FORMAT_TO_UTC_DATE = 2

def calculate_duration(start_str: str, end_str: str) -> str:
    end = datetime.strptime(end_str, "%Y%m%d %H:%M:%S")
    if start_str:
        start = datetime.strptime(start, "%Y%m%d %H:%M:%S")
    else:
        start = end - timedelta(days=30) 
    
    duration = end - start
    if duration.days > 365:
        return f"{duration.days // 365} Y"
    elif duration.days > 30:
        return f"{duration.days // 30} M"
    elif duration.days > 0:
        return f"{duration.days} D"
    elif duration.seconds > 3600:
        return f"{duration.seconds // 3600} H"
    elif duration.seconds > 60:
        return f"{duration.seconds // 60} min"
    else:
        return f"{duration.seconds} S"

def get_bars(symbol: str, ib: IB, duration: str):
    contract = Stock(symbol, 'SMART', 'USD')
    #contract = Stock('HM.B', 'SFB', 'SEK')
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='2 D',
        barSizeSetting='30 mins', whatToShow='MIDPOINT', useRTH=True, formatDate=FORMAT_TO_UTC_DATE)

    df = util.df(bars)
    df.to_csv(f"{symbol}.csv")
    logger.info(f"Wrote {len(df)} lines to {symbol}.csv")

def download(symbols, file, timeframe, verbose, start, tz='America/New_York', id=0, host='127.0.0.1', port=7498):
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
    
    duration = calculate_duration(start, f"{datetime.now():%Y%m%d %H:%M:%S}")
    #print(duration)
    for sym in symbols:
        get_bars(sym, ib, duration=duration)


def main():
    # set logging to debug
    logging.basicConfig(level=logging.INFO)
    #logging.getLogger('ib_insync').setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', default="SPY", help="Comma separated list of symbols", dest='symbols')
    parser.add_argument('--file', help='Read symbols from file', dest='file')
    parser.add_argument('--timeframe', default='5min', dest='timeframe')
    parser.add_argument('-v', '--verbose', action='count', dest='verbose')
    parser.add_argument('--start', dest='start')
    parser.add_argument('--tz', default='America/New_York', dest='tz')
    parser.add_argument('--id', default='0', dest='id')
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=7498, type=int, dest='port')

    args = parser.parse_args()
    download(args.symbols, args.file, args.timeframe, args.verbose, args.start, args.tz, args.id, args.host, args.port)


if __name__ == '__main__':
    main()