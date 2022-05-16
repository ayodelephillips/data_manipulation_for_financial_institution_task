date = 'Date'
fund_name = 'Fund Name'
number_of_units_held = 'Number of Units Held'
unit_price = 'Price of Unit'
aum_value_of_fund_units = 'AUM(value of fund units)'
employee_id = 'Employee id'
employee_gender = 'Employee Gender'
industry_worked_at = 'Industry worked at'
fund_id = 'Fund Id'
employee_salary = 'employee salary'
number_of_employee = 'no of employee'
average_employee_salary = 'average employee salary'
salary_difference = 'salary difference'


UNITS_PER_FUND_PER_EMPLOYEE_COLUMNS = [date, fund_name, number_of_units_held, employee_id]
UNIT_PRICE_OF_FUND_PER_DAY_COLUMNS = [date, unit_price, fund_id]
AUM_PER_FUND_UNITS_PER_DAY_COLUMNS = [date, fund_id, fund_name, aum_value_of_fund_units]
AVERAGE_SALARY_PER_EMPLOYEE_GENDER_COLUMNS = [employee_gender, number_of_employee,average_employee_salary, salary_difference]
INDUSTRIES_EMPLOYING_GENDER_COLUMNS = [employee_gender, industry_worked_at, number_of_employee]


EMPLOYEE_FILE_COLS = ['id', 'gender', 'region', 'salary', 'DOB', 'industries']
FUND_PRICES_COLS = ['id', 'amount_on', 'fund_id', 'unit_value_pounds']
FUNDS_COLS = ['id', 'Fund Name']
INVESTMENT_COLS = ['id', 'state', 'fund_id', 'fund_price_on', 'type', 'fund_units', 'amount_pennies', 'employee_id']