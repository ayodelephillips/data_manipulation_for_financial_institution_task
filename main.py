import datetime
import os
from abc import ABC
import pandas as pd
import numpy as np
from helper import (EMPLOYEE_FILE_TOKEN, FUND_PRICES_FILE_TOKEN, FUNDS_FILE_TOKEN,
                    INVESTMENT_FILE_TOKEN, get_recent_working_day)
from columns import *
import logging
logging.basicConfig(level=logging.INFO)

DATE_FORMAT = '%Y-%m-%d'


class Pipeline(ABC):
    """ Generic pipeline class"""
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    @staticmethod
    def load(file_token, file_path):
        df = pd.read_csv(f'{file_path}/{file_token}')
        return df

    def read_data(self):
        raise NotImplementedError

    def process_data(self, pipeline_date):
        raise NotImplementedError

    def write_data(self):
        raise NotImplementedError

    @staticmethod
    def date_range(start_date, end_date):
        for d in range((end_date-start_date).days):
            yield start_date+datetime.timedelta(d)

    def run_pipeline(self):
        self.read_data()
        for pipeline_date in self.date_range(self.start_date, self.end_date):
            self.process_data(pipeline_date)
        self.write_data()


class AUM(Pipeline):
    def __init__(self, start_date, end_date):
        # store data loaded from files, or got from joining other dfs
        self.investments_df = None
        self.fund_price_df = None
        self.fund_df = None
        self.employee_df = None
        self.invest_employee_df = None
        self.aum_df = None

        # store accumulated data across each day's run
        self.units_per_fund_per_employee_df = None  # accumulated units per fund across each run
        self.unit_price_fund_df = None  # accumulated unit price per fund for each day
        self.aum_per_fund_df = None  # accumulated aum value alongside the fun details for each day
        self.employee_gender_and_average_salary = None
        self.industry_and_gender_df = None

        self.pipeline_date = None
        self.alternative_date = None  # always same as pipeline date except for weekends
        self.input_file_directory_path = f"{os.getcwd()}/files/input_dir"
        self.output_file_directory_path = f"{os.getcwd()}/files/output_dir"
        super(AUM, self).__init__(start_date, end_date)

    def read_data(self):
        """ load the files, and merge relevant dfs"""
        # load investment, employee, fund data
        self.investments_df = self.load(INVESTMENT_FILE_TOKEN, self.input_file_directory_path)
        self.fund_df = self.load(FUNDS_FILE_TOKEN, self.input_file_directory_path)
        self.employee_df = self.load(EMPLOYEE_FILE_TOKEN, self.input_file_directory_path)
        self.fund_price_df = self.load(FUND_PRICES_FILE_TOKEN, self.input_file_directory_path)

        # merge investment with the employee df
        self.invest_employee_df = pd.merge(
            left=self.investments_df, right=self.employee_df, how='left',
            left_on='employee_id', right_on='id', suffixes=('', '_employee')
        )
        self.invest_employee_df = pd.merge(
            left=self.invest_employee_df, right=self.fund_df, how='left',
            left_on='fund_id', right_on='id', suffixes=('', '_drop'))

        self.process_invest_employee_df_after_load()

        self.fund_price_df['amount_on'] = pd.to_datetime(
            self.fund_price_df['amount_on'], infer_datetime_format=True).dt.strftime(DATE_FORMAT)
        self.aum_df = pd.merge(left=self.invest_employee_df, right=self.fund_price_df, how="inner",
                               left_on=['fund_id', 'fund_price_on'], right_on=["fund_id", "amount_on"],
                               suffixes=('', '_drop'))
        self.aum_df[aum_value_of_fund_units] = self.aum_df['fund_units'] * self.aum_df['unit_value_pounds']

    def process_invest_employee_df_after_load(self):
        # filter out the rejected investments
        self.invest_employee_df = self.invest_employee_df[self.invest_employee_df["state"].isin(["completed"])]

        # group by fund, then by employee and type
        self.invest_employee_df = self.invest_employee_df.groupby(
            ["fund_id", "employee_id", "type"], as_index=False).agg({"fund_units": "sum",
                                                                     "fund_price_on": "first", "Fund Name": "first"})
        self.invest_employee_df = self.invest_employee_df.sort_values(by=["fund_id", "employee_id",
                                                                          "fund_price_on", "type"])

        # get the difference with each groupings' 'types' - subtract 'sells' from 'buys'
        self.invest_employee_df["fund_unit_diff"] = self.invest_employee_df.groupby(
            ["fund_id", "employee_id", "fund_price_on"])["fund_units"].diff().fillna(
            self.invest_employee_df["fund_units"])
        self.invest_employee_df = self.invest_employee_df.drop_duplicates(
            subset=["fund_id", "employee_id", "fund_price_on"], keep='last')
        self.invest_employee_df["fund_units"] = self.invest_employee_df["fund_unit_diff"]
        self.invest_employee_df.drop(columns=["fund_unit_diff"], inplace=True)
        logging.info("The investment_employee df has been created!!!")

    @staticmethod
    def filter_data_by_date(df, date_col, date_to_filter):
        return df[df[date_col] == date_to_filter]

    @staticmethod
    def drop_unwanted_cols(df, expected_output_cols):
        df = df.drop(columns=[col for col in df if col not in expected_output_cols])
        return df

    def number_of_units_per_fund_per_employee(self):
        df = self.filter_data_by_date(df=self.invest_employee_df, date_col="fund_price_on",
                                      date_to_filter=self.pipeline_date.strftime(DATE_FORMAT))
        return_df = pd.DataFrame(columns=UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS)
        if not df.empty:
            return_df[fund_name] = df['Fund Name']
            return_df[number_of_units_held] = abs(df['fund_units'])
            return_df[employee_id] = df['employee_id']
        else:
            row = pd.DataFrame([[np.nan, np.nan, np.nan, np.nan]], columns=UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS)
            return_df = pd.concat([return_df, row])
        return_df[date] = self.pipeline_date.strftime(DATE_FORMAT)
        return return_df

    def unit_price_per_fund(self):
        df = self.fund_price_df.copy()
        # filter
        df['amount_on'] = pd.to_datetime(df['amount_on'], infer_datetime_format=True).dt.strftime(DATE_FORMAT)
        df = self.filter_data_by_date(df=df, date_col="amount_on",
                                      date_to_filter=self.alternative_date.strftime(DATE_FORMAT))
        if df.empty:
            raise ValueError("There should always be data for each calendar day")

        df = df.rename(columns={"fund_id": fund_id, "unit_value_pounds": unit_price})
        df[date] = self.pipeline_date.strftime(DATE_FORMAT)
        # sort and reorder cols
        df = df.sort_values(by=[date, fund_id])[UNIT_PRICE_OF_FUND_PER_DAY_COLUMNS]
        return self.drop_unwanted_cols(df, UNIT_PRICE_OF_FUND_PER_DAY_COLUMNS)

    def aum_per_fund_per_day(self):
        df = self.aum_df.copy()
        df = self.filter_data_by_date(df=df, date_col="amount_on",
                                      date_to_filter=self.alternative_date.strftime(DATE_FORMAT))
        return_df = pd.DataFrame(columns=AUM_PER_FUND_UNITS_PER_DAY_COLUMNS)
        if not df.empty:
            return_df = df.rename(columns={"fund_id": fund_id, "unit_value_pounds": unit_price})
        else:
            row = pd.DataFrame([[np.nan, np.nan, np.nan, np.nan]], columns=UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS)
            return_df = pd.concat([return_df, row])
        return_df[date] = self.pipeline_date.strftime(DATE_FORMAT)
        # sort and reorder cols
        return_df = return_df.sort_values(by=[date, fund_id])[AUM_PER_FUND_UNITS_PER_DAY_COLUMNS]
        return self.drop_unwanted_cols(return_df, AUM_PER_FUND_UNITS_PER_DAY_COLUMNS)

    def average_salary_per_gender(self):
        df = self.employee_df.copy()
        df = df.groupby(['gender'])['salary'].agg(['sum', 'count', 'mean']).reset_index()
        df['salary_diff'] = df['mean'].diff()
        df = df.rename(columns={'gender': employee_gender, 'sum': number_of_employee, 'mean': average_employee_salary,
                       'salary_diff': salary_difference})
        return self.drop_unwanted_cols(df, AVERAGE_SALARY_PER_EMPLOYEE_GENDER_COLUMNS)

    def sector_employing_certain_gender_more(self):
        df = self.employee_df.copy()
        df = df.groupby(['gender', 'industries'])['id'].agg(['count']).reset_index()
        df = df.rename(columns={'gender': employee_gender, 'industries': industry_worked_at,
                                'count': number_of_employee})
        return self.drop_unwanted_cols(df, INDUSTRIES_EMPLOYING_GENDER_COLUMNS)

    def process_data(self, pipeline_date):
        self.pipeline_date = pipeline_date
        self.alternative_date = get_recent_working_day(self.pipeline_date)
        if self.pipeline_date != self.alternative_date:
            logging.info(f"Date {self.pipeline_date} falls on weekend,  alternative date is {self.alternative_date}")

        self.units_per_fund_per_employee_df = pd.concat(
            [self.units_per_fund_per_employee_df, self.number_of_units_per_fund_per_employee()], ignore_index=True)
        self.unit_price_fund_df = pd.concat(
            [self.unit_price_fund_df, self.unit_price_per_fund()], ignore_index=True)
        self.aum_per_fund_df = pd.concat(
            [self.aum_per_fund_df, self.aum_per_fund_per_day()], ignore_index=True)
        if self.employee_gender_and_average_salary is None:
            self.employee_gender_and_average_salary = self.average_salary_per_gender()
        if self.industry_and_gender_df is None:
            self.industry_and_gender_df = self.sector_employing_certain_gender_more()

    def write_data(self):
        self.units_per_fund_per_employee_df.fillna(method='ffill').fillna(
            method='bfill').to_csv(f'{self.output_file_directory_path}/units_per_fund_per_employee.csv', index=False)
        logging.info(f"Written to {self.output_file_directory_path}/units_per_fund_per_employee.csv")

        self.unit_price_fund_df.to_csv(f'{self.output_file_directory_path}/unit_price_per_fund_per_day.csv',
                                       index=False)
        logging.info(f"Written to {self.output_file_directory_path}/unit_price_per_fund_per_day.csv")

        self.aum_per_fund_df.fillna(method='ffill').fillna(
            method='bfill').to_csv(f'{self.output_file_directory_path}/aum_per_fund_per_day.csv', index=False)
        logging.info(f"Written to {self.output_file_directory_path}/aum_per_fund_per_day.csv. "
                     f"Data is copied from closest date when unavailable")

        self.employee_gender_and_average_salary.to_csv(
            f'{self.output_file_directory_path}/employee_gender_mean_salary.csv', index=False)
        logging.info(f"Written to {self.output_file_directory_path}/employee_gender_mean_salary.csv")

        self.industry_and_gender_df.to_csv(
            f'{self.output_file_directory_path}/industries_employing_gender.csv', index=False)
        logging.info(f"Written to {self.output_file_directory_path}/industries_employing_gender.csv")


if __name__ == '__main__':
    aum = AUM(datetime.datetime(2020, 2, 24), datetime.datetime(2020, 9, 1))
    aum.run_pipeline()
