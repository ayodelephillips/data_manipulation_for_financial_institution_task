import pytest
import datatest as dt
import pandas as pd
import os
from columns import *
from main import DATE_FORMAT
from columns import UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS, UNIT_PRICE_OF_FUND_PER_DAY_COLUMNS, \
    AUM_PER_FUND_UNITS_PER_DAY_COLUMNS, AVERAGE_SALARY_PER_EMPLOYEE_GENDER_COLUMNS, INDUSTRIES_EMPLOYING_GENDER_COLUMNS
input_file_directory_path = f"{os.getcwd()}/files/output_dir"
date_range = pd.date_range('24/02/2020', '31/08/2020', freq='d').strftime(DATE_FORMAT)

# TODOs - replace with mockings to avoid using the output file


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def units_per_fund_per_employee_df():
    print(f'{input_file_directory_path}/units_per_fund_per_employee.csv')
    return pd.read_csv(f'{input_file_directory_path}/units_per_fund_per_employee.csv')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def unit_price_per_fund_df():
    return pd.read_csv(f'{input_file_directory_path}/unit_price_per_fund_per_day.csv')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def aum_per_fund_df():
    return pd.read_csv(f'{input_file_directory_path}/aum_per_fund_per_day.csv')


def test_units_per_fund_output(units_per_fund_per_employee_df):
    """
    test output columns,
    ensure all calendar dates are in the output file
    """
    dt.validate(units_per_fund_per_employee_df.columns, UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS)
    diff = list(set(date_range) - set(units_per_fund_per_employee_df[date].unique()))
    neg_fund_units = [unit for unit in list(units_per_fund_per_employee_df[number_of_units_held].unique()) if unit<0]
    assert len(diff) == 0
    assert len(neg_fund_units) == 0


def test_unit_price_per_fund_output(unit_price_per_fund_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(unit_price_per_fund_df.columns, UNIT_PRICE_OF_FUND_PER_DAY_COLUMNS)
    diff = list(set(date_range) - set(unit_price_per_fund_df[date].unique()))
    assert len(diff) == 0


def test_aum_output(aum_per_fund_df):
    """
    test that  the columns in  the files are the same
    """
    dt.validate(aum_per_fund_df.columns, AUM_PER_FUND_UNITS_PER_DAY_COLUMNS)
    diff = list(set(date_range) - set(aum_per_fund_df[date].unique()))
    assert len(diff) == 0
