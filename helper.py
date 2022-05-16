import datetime
import logging
logging.basicConfig(level=logging.INFO)

EMPLOYEE_FILE_TOKEN = 'Employees.csv'
FUND_PRICES_FILE_TOKEN = 'Fund prices.csv'
FUNDS_FILE_TOKEN = 'Funds.csv'
INVESTMENT_FILE_TOKEN = 'Investments.csv'


def is_date_weekend(pipeline_date: datetime.datetime) -> bool:
    if pipeline_date.weekday() > 4:
        return True
    return False


def get_recent_working_day(date):
    """ returns date if date is a working day, else returns most recent working day"""
    if is_date_weekend(date):
        offset = max(1, (date.weekday() + 6) % 7 - 3)
        timedelta = datetime.timedelta(offset)
        return date - timedelta
    return date




