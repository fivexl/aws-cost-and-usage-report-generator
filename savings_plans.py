from logging import Logger

import pandas as pd
from mypy_boto3_ce import CostExplorerClient

import utils
from config import (
    FIRST_DAY_PREV_MONTH,
    FIRST_DAY_THIS_MONTH,
    GET_SAVINGS_PLANS_INFO,
    MISSING_DATA_PLACEHOLDER,
    SP_CONFIG,
)


#####################################################################################
#####################################################################################
# Savings Plans Utilization
def get_savings_plans_utilization_details(
    client: CostExplorerClient, logger: Logger
) -> dict | None:
    try:
        return client.get_savings_plans_utilization_details(
            TimePeriod={"Start": FIRST_DAY_PREV_MONTH, "End": FIRST_DAY_THIS_MONTH},
        )  # type: ignore
    except client.exceptions.DataUnavailableException as e:
        logger.info("Savings Plans utilization data is not available. %s", e)
        return None
    except Exception as e:
        logger.warning("Failed to get Savings Plans utilization data. %s", e)
        return None


def get_savings_plans_utilization_df(
    client: CostExplorerClient,
    logger: Logger,
    org_client
) -> pd.DataFrame | None:
    details = get_savings_plans_utilization_details(client, logger)

    if details is None:
        return None

    raw_utilization_df = utilization_details_to_df(
        details["SavingsPlansUtilizationDetails"],
        org_client
    )

    raw_total_utilization = details["Total"]
    return format_savings_plans_utilizations(
        raw_utilization_df, raw_total_utilization, MISSING_DATA_PLACEHOLDER
    )


def utilization_details_to_df(sp_info: dict, org_client) -> pd.DataFrame:
    data = [
        {
            "SavingsPlanArn": sp["SavingsPlanArn"],
            "Utilization": sp["Utilization"]["UtilizationPercentage"],
            "EndDateTime": sp["Attributes"]["EndDateTime"],
            "Account": utils.get_account_info_by_account_name(
                account_name = sp["Attributes"]["AccountName"],
                org_client = org_client
            ),
            "Region": sp["Attributes"]["Region"],
            "Savings": sp["Savings"]["NetSavings"],
            "Type": sp["Attributes"]["SavingsPlansType"],
        }
        for sp in sp_info
    ]
    return pd.DataFrame(data)


def format_savings_plans_utilizations(
    raw_df: pd.DataFrame, total: dict, MISSING_DATA_PLACEHOLDER
) -> pd.DataFrame:
    df = raw_df.copy()

    df["DaysUntilEnd"] = df["EndDateTime"].apply(utils.days_until)
    df["Id"] = df["SavingsPlanArn"].apply(lambda x: x.split("/")[-1])
    total_row = {
        "Id": "Total",
        "Utilization": total["Utilization"]["UtilizationPercentage"],
        "DaysUntilEnd": MISSING_DATA_PLACEHOLDER,
        "Account": MISSING_DATA_PLACEHOLDER,
        "Region": MISSING_DATA_PLACEHOLDER,
        "Savings": total["Savings"]["NetSavings"],
        "Type": MISSING_DATA_PLACEHOLDER,
    }
    df.loc[len(df.index)] = total_row  # type: ignore
    df["Utilization"] = utils.to_percentage(df["Utilization"])
    df["Savings"] = utils.to_dollars(df["Savings"])
    return df[
        ["Id", "Utilization", "Savings", "DaysUntilEnd", "Account", "Region", "Type"]
    ]


#####################################################################################
#####################################################################################
# Savings Plans Coverage
def get_savings_plans_coverage_info(
    client: CostExplorerClient,
    logger: Logger,
) -> dict | None:
    try:
        return client.get_savings_plans_coverage(
            TimePeriod={"Start": FIRST_DAY_PREV_MONTH, "End": FIRST_DAY_THIS_MONTH},
            GroupBy=[
                {"Type": "DIMENSION", "Key": "REGION"},
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "INSTANCE_TYPE_FAMILY"},
            ],  # type: ignore
            Granularity="MONTHLY",
        )
    except client.exceptions.DataUnavailableException as e:
        logger.info("Savings Plans utilization data is not available. %s", e)
        return None
    except Exception as e:
        logger.warning("Failed to get Savings Plans utilization data. %s", e)
        return None


def get_savings_plans_coverage_df(
    client: CostExplorerClient, logger: Logger
) -> pd.DataFrame | None:
    coverage_info = get_savings_plans_coverage_info(client, logger)

    if coverage_info is None:
        return None

    raw_coverage_df = coverages_to_df(coverage_info["SavingsPlansCoverages"])
    return format_savings_plans_coverage(raw_coverage_df)


def coverages_to_df(
    coverages: dict, missing_data_placeholder: str = ""
) -> pd.DataFrame:
    data = []
    for coverage in coverages:
        instance_type_family = coverage["Attributes"]["INSTANCE_TYPE_FAMILY"]
        instance_type_family = (
            instance_type_family
            if instance_type_family != "NoInstanceTypeFamily"
            else missing_data_placeholder
        )
        data.append(
            {
                "Service": coverage["Attributes"]["SERVICE"],
                "Coverage": coverage["Coverage"]["CoveragePercentage"],
                "InstanceTypeFamily": instance_type_family,
                "Region": coverage["Attributes"]["REGION"],
            }
        )

    return pd.DataFrame(data)


def format_savings_plans_coverage(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["Coverage"] = utils.to_percentage(df["Coverage"])
    return df


#####################################################################################
#####################################################################################
# Savings Plans Purchase Recommendations
def get_savings_plans_purchase_recommendations_info(
    c: CostExplorerClient, input: list[dict], logger: Logger
) -> list[dict]:
    spprs = []
    for sppr_input in input:
        try:
            r = c.get_savings_plans_purchase_recommendation(**sppr_input)[
                "SavingsPlansPurchaseRecommendation"
            ]
        except c.exceptions.DataUnavailableException:
            logger.info(
                "Purchase recommendations data is unavailable for %s", sppr_input
            )
            continue
        except Exception:
            logger.warning(
                "Error while getting purchase recommendations for %s", sppr_input
            )
            continue
        pr_dict = savings_plans_purchase_recommendations_to_dict(r)  # type: ignore
        if pr_dict is not None:
            spprs.append(pr_dict)
    return spprs


def get_savings_plans_purchase_recommendations_df(
    client: CostExplorerClient,
    logger: Logger,
) -> pd.DataFrame | None:
    raw_spprs = get_savings_plans_purchase_recommendations_info(
        client, SP_CONFIG, logger
    )

    if not raw_spprs:
        return None
    raw_spprs_df = pd.DataFrame(raw_spprs, index=None)
    return format_saving_plan_recommendations(raw_spprs_df)


def savings_plans_purchase_recommendations_to_dict(
    recommendations: dict,
) -> dict | None:
    if summary := recommendations.get("SavingsPlansPurchaseRecommendationSummary"):
        currency = summary.get("CurrencyCode")
        estimated_monthly_savings = summary.get("EstimatedMonthlySavingsAmount")
        if estimated_monthly_savings is None:
            return

        estimated_monthly_savings = float(estimated_monthly_savings)
        spt = utils.humanize_savings_plans_type(recommendations["SavingsPlansType"])
        lookback_period = recommendations["LookbackPeriodInDays"]

        return {
            "EstimatedMonthlySavings": estimated_monthly_savings,
            "Currency": currency,
            "LookbackPeriodInDays": lookback_period,
            "SavingsPlansType": spt,
        }


def format_saving_plan_recommendations(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["EstimatedMonthlySavings"] = utils.to_dollars(df["EstimatedMonthlySavings"])
    df["LookbackPeriodInDays"] = df["LookbackPeriodInDays"].apply(
        utils.humanize_lookback_period
    )
    return df[["SavingsPlansType", "EstimatedMonthlySavings", "LookbackPeriodInDays"]]


def get_savings_plans_dataframes(
    client: CostExplorerClient,
    logger: Logger,
    org_client
) -> dict[str, dict[str, pd.DataFrame | None]] | None:
    if GET_SAVINGS_PLANS_INFO:
        savings_plans_utilization_df = get_savings_plans_utilization_df(
            client,
            logger,
            org_client
        )
        savings_plans_coverage_df = get_savings_plans_coverage_df(client, logger)
        if savings_plans_utilization_df is None:
            savings_plans_coverage_df = None
        return {
            "Savings plans info": {
                "Savings plans utilization details": savings_plans_utilization_df,
                "Savings plans coverage": savings_plans_coverage_df,
                "Savings plans purchase recommendations": get_savings_plans_purchase_recommendations_df(
                    client, logger
                ),
            }
        }
