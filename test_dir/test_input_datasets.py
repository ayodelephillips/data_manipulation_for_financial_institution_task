"""
Tests to check that input data is as expected because datasets could change
"""
import pytest
import datatest as dt
import pandas as pd
import os
from helper import (EMPLOYEE_FILE_TOKEN, FUND_PRICES_FILE_TOKEN, FUNDS_FILE_TOKEN,
                    INVESTMENT_FILE_TOKEN)
from columns import EMPLOYEE_FILE_COLS, FUND_PRICES_COLS, FUNDS_COLS, INVESTMENT_COLS
input_file_directory_path = f"{os.getcwd()}/files/input_dir"


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def employee_df():
    return pd.read_csv(f'{input_file_directory_path}/{EMPLOYEE_FILE_TOKEN}')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def fund_prices_df():
    return pd.read_csv(f'{input_file_directory_path}/{FUND_PRICES_FILE_TOKEN}')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def fund_df():
    return pd.read_csv(f'{input_file_directory_path}/{FUNDS_FILE_TOKEN}')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def investment_df():
    return pd.read_csv(f'{input_file_directory_path}/{INVESTMENT_FILE_TOKEN}')


def test_columns_in_employee_file(employee_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(employee_df.columns, EMPLOYEE_FILE_COLS)


def test_columns_in_fund_price_file(fund_prices_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(fund_prices_df.columns, FUND_PRICES_COLS)
    # assert that all fund price data has all working days
    date_range = pd.date_range('24/02/2020', '31/08/2020', freq='B').strftime('%d/%m/%Y')
    diff = list(set(date_range) - set(fund_prices_df['amount_on'].unique()))
    assert len(diff) == 0


def test_columns_in_fund_file(fund_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(fund_df.columns, FUNDS_COLS)


def test_columns_in_investment_file(investment_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(investment_df.columns, INVESTMENT_COLS)
