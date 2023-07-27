import datetime
from dateutil.relativedelta import relativedelta

now = datetime.datetime.now()
first_day_this_month_raw = datetime.datetime(now.year, now.month, 1)
first_day_prev_month_raw = first_day_this_month_raw - relativedelta(months=1)

# If set to false, script will not get any data.
GET_SAVINGS_PLANS_INFO = True
GET_RESERVED_INSTANCES_INFO = True

MISSING_DATA_PLACEHOLDER = "" 

# Start and end dates for getting data for Savings Plans & Reserved Instances
# Now it defaults to the first day of the current month and the first day of the previous month.
# You can change the dates to whatever you want, but if range would be more 
# than 30 days, Savings Plans utilization info would be duplicated for each month.
FIRST_DAY_THIS_MONTH = first_day_this_month_raw.strftime("%Y-%m-%d")
FIRST_DAY_PREV_MONTH = first_day_prev_month_raw.strftime("%Y-%m-%d")


# Configuration for getting purchase recommendations for Savings Plans
SP_CONFIG = [
    {
        "SavingsPlansType": "COMPUTE_SP",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "SavingsPlansType": "EC2_INSTANCE_SP",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "SavingsPlansType": "SAGEMAKER_SP",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
]

# Configuration for getting purchase recommendations for Reserved Instances
RPR_CONFIG = [
    {
        "Service": "Amazon Elastic Compute Cloud - Compute",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "Service": "Amazon Relational Database Service",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "Service": "Amazon ElastiCache",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "Service": "Amazon Redshift",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
    {
        "Service": "Amazon Elasticsearch Service",
        "LookbackPeriodInDays": "THIRTY_DAYS",
        "TermInYears": "ONE_YEAR",
        "PaymentOption": "NO_UPFRONT",
        "AccountScope": "PAYER",
    },
]
