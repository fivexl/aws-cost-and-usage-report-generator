from datetime import datetime, timezone

import pandas as pd

from config import MISSING_DATA_PLACEHOLDER

def days_until(date_str: str) -> str:
    """Return the number of days until the date_str.
    Date_str must be in the format of 2021-09-30T00:00:00.000Z
    """
    date_object = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    )
    now = datetime.now(timezone.utc)

    # Calculate difference in days
    diff_days = (date_object - now).days

    if diff_days < 1:
        return "Less than a day"
    elif diff_days == 1:
        return "1 day"
    else:
        return f"{diff_days} days"


_HUMANIZED_LOOKBACK_PERIOD_IN_DAYS = {
    "SEVEN_DAYS": "7 days",
    "SIXTY_DAYS": "60 days",
    "THIRTY_DAYS": "30 days",
}


def humanize_lookback_period(lookback_period: str) -> str:
    return _HUMANIZED_LOOKBACK_PERIOD_IN_DAYS[lookback_period]


_HUMANIZED_SAVINGS_PLANS_TYPE = {
    "COMPUTE_SP": "Compute Savings Plans",
    "EC2_INSTANCE_SP": "EC2 Instance Savings Plans",
    "SAGEMAKER_SP": "SageMaker Savings Plans",
}


def humanize_savings_plans_type(savings_plans_type: str) -> str:
    return _HUMANIZED_SAVINGS_PLANS_TYPE[savings_plans_type]


def to_percentage(series: pd.Series, round: int = 1) -> pd.Series:
    series = series.astype(float).round(round)
    return series.astype(str) + "%"  # sourcery skip: use-fstring-for-concatenation


def to_dollars(series: pd.Series, round: int = 1) -> pd.Series:
    series = series.astype(float).round(round)
    return "$" + series.astype(str)  # sourcery skip: use-fstring-for-concatenation


def get_account_info_by_account_name(account_name: str, org_client) -> str:
    accounts = org_client.list_accounts()
    return next(
        (
            f"{account_name}({account['Id']})"
            for account in accounts["Accounts"]
            if account["Name"] == account_name
        ),
        account_name,
    )