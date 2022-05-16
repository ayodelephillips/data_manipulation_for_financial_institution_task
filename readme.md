`
The task is to create a table or csv extract containing

1. The number of units held in a given fund on a given day
2. The unit price of the fun on that day
3. The AUM of those fund units on the day
4.The name of the fund the units and AUM relate to
5.Do men earn more on average/median than women?
6.How much difference is there?
7.Which sectors employ women more?
8.Is there a way to predict gender pay gap? can we build a statistical model?

The output dataset should include all calendar days. use recent day's price if no fund price is available The fund units
in the investments table is always positive. You need to add all buy/switch buy investments and subtract all sell/switch
sell investments
`

**Things to note**
1. The input files - Employees.csv, Funds.csv, etc. are stored in the **Files/input_dir** folder
2. There are 5 output files. They are stored in  **Files/output_dir** folder
 - aum_per_fund_per_day.csv contains the data for the aum alongside its fund name
 - employee_gender_mean_salary.csv file contains information about earnings, gender, mean earning, difference in earnings
3. There is a statistical model present in **statistical_model/model.py**. A dataset called 
**UK Gender Pay Gap Data - 2021 to 2022.csv** is used to train a linear regression model. The dataset is obtained from 
[https://gender-pay-gap.service.gov.uk/viewing/download](https://gender-pay-gap.service.gov.uk/viewing/download).
The model is developed using [https://www.tensorflow.org/tutorials/keras/regression](https://www.tensorflow.org/tutorials/keras/regression)
4. Tests are present in the **test_dir** folder

To run the program, do the following
1. Create a virtual environment using the requirements.txt
2. Activate the virtual environment
3. Navigate to the root folder(solutions.py)
4. Enter `python main.py`

To run the model creation, which will train and output the model, do the following
1. while still within the activated virtual environment,
2. navigate to **statistical_model** directory
3. Enter `python model.py`
