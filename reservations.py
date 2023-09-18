from logging import Logger

import pandas as pd
from mypy_boto3_ce import CostExplorerClient

import utils
from config import (
    FIRST_DAY_PREV_MONTH,
    FIRST_DAY_THIS_MONTH,
    GET_RESERVED_INSTANCES_INFO,
    MISSING_DATA_PLACEHOLDER,
    RPR_CONFIG,
    LIST_OF_SERVICES_FOR_RESERVATIONS_COVERAGE
)


#####################################################################################
#####################################################################################
# Reservations Utilization
def get_reservations_utilizations_data(
    client: CostExplorerClient, logger: Logger
) -> dict | None:
    logger.info("Getting reservations utilization data for time period from: %s to: %s")
    try:
        return client.get_reservation_utilization(
            TimePeriod={
                "Start": FIRST_DAY_PREV_MONTH,
                "End": FIRST_DAY_THIS_MONTH,
            },
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SUBSCRIPTION_ID"},
            ],
        )  # type: ignore
    except client.exceptions.DataUnavailableException:
        logger.info(
            "There is no reservations utilization info for time period from: %s to: %s",
            FIRST_DAY_PREV_MONTH,
            FIRST_DAY_THIS_MONTH,
        )
        return None
    except Exception as e:
        logger.warning(
            "Failed to get reservation utilization data for time period from: %s to: %s, error: %s",
            FIRST_DAY_PREV_MONTH,
            FIRST_DAY_THIS_MONTH,
            e,
        )
        return None


def reservations_utilization_to_df(reservations_utilization_data: dict, org_client) -> pd.DataFrame:

    data = [
        {
            "Id": group["Attributes"]["reservationARN"],
            "Account": utils.get_account_info_by_account_name(
                account_name = group["Attributes"]["accountName"],
                org_client = org_client
            ),
            "InstanceType": group["Attributes"]["instanceType"],
            "Region": group["Attributes"]["region"],
            "UtilizationPercentage": group["Utilization"]["UtilizationPercentage"],
            "Savings": group["Utilization"]["NetRISavings"],
            "DaysUntilEnd": group["Attributes"]["endDateTime"],
        }
        for group in reservations_utilization_data
    ]
    return pd.DataFrame(data)


def format_reservations_utilization_df(raw_df, raw_total: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    df["DaysUntilEnd"] = df["DaysUntilEnd"].apply(utils.days_until)
    df["Id"] = df["Id"].apply(lambda x: x.split("/")[-1])

    df["UtilizationPercentage"] = df["UtilizationPercentage"].astype(float)

    df = df.sort_values(by="UtilizationPercentage", ascending=False)

    total_row = {
        "Id": "Total",
        "UtilizationPercentage": raw_total["UtilizationPercentage"],
        "Savings": raw_total["NetRISavings"],
        "DaysUntilEnd": MISSING_DATA_PLACEHOLDER,
        "Account": MISSING_DATA_PLACEHOLDER,
        "InstanceType": MISSING_DATA_PLACEHOLDER,
        "Region": MISSING_DATA_PLACEHOLDER,
    }
    df.loc[len(df.index)] = total_row  # type: ignore
    df["UtilizationPercentage"] = utils.to_percentage(df["UtilizationPercentage"])
    df["Savings"] = utils.to_dollars(df["Savings"])
    return df[
        [
            "Id",
            "UtilizationPercentage",
            "Savings",
            "DaysUntilEnd",
            "Account",
            "Region",
            "InstanceType",
        ]
    ]


def get_reservations_utilization_df(
    client: CostExplorerClient,
    logger: Logger,
    org_client,
) -> pd.DataFrame | None:
    reservation_utilization_data = get_reservations_utilizations_data(client, logger)
    logger.debug("Reservation utilization data: %s", reservation_utilization_data)

    if reservation_utilization_data is None:
        return None
    if reservation_utilization_data["UtilizationsByTime"][0]["Groups"] == []:
        logger.info("Reservation utilization data is empty")
        return None

    raw_reservation_utilization_df = reservations_utilization_to_df(
        reservation_utilization_data["UtilizationsByTime"][0]["Groups"],
        org_client
    )
    logger.debug("Raw reservation utilization df: %s", raw_reservation_utilization_df)

    raw_total = reservation_utilization_data["UtilizationsByTime"][0]["Total"]
    logger.debug("Raw total: %s", raw_total)
    return format_reservations_utilization_df(raw_reservation_utilization_df, raw_total)


#####################################################################################
#####################################################################################
# Reservations Coverage


def get_reservation_coverage_data(
    client: CostExplorerClient, logger: Logger, service: str
) -> dict | None:
    logger.info("Getting reservation coverage data for %s to %s", FIRST_DAY_PREV_MONTH, FIRST_DAY_THIS_MONTH)
    try:
        return client.get_reservation_coverage(
            TimePeriod={
                "Start": FIRST_DAY_PREV_MONTH,
                "End": FIRST_DAY_THIS_MONTH,
            },
            GroupBy=[
                {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
            ],
            Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': [service],
                    },
                }
        )  # type: ignore
    except client.exceptions.DataUnavailableException:
        logger.info(
            "There is no reservations coverage info for time period from: %s to: %s",
            FIRST_DAY_PREV_MONTH,
            FIRST_DAY_THIS_MONTH,
        )
        return None
    except Exception as e:
        logger.warning(
            "Failed to get reservation coverage data for time period from: %s to: %s, error: %s",
            FIRST_DAY_PREV_MONTH,
            FIRST_DAY_THIS_MONTH,
            e,
        )
        return None


def reservation_coverage_to_df(
    reservation_coverage_data: dict,
) -> pd.DataFrame:
    data = [
        {
            "instanceType": group["Attributes"]["instanceType"],
            "CoverageHoursPercentage": group["Coverage"]["CoverageHours"][
                "CoverageHoursPercentage"
            ],
        }
        for group in reservation_coverage_data["CoveragesByTime"][0]["Groups"]
    ]
    return pd.DataFrame(data)


def format_reservation_coverage_df(raw_df, total_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["CoverageHoursPercentage"] = df["CoverageHoursPercentage"].astype(float)
    df = df.sort_values(by="CoverageHoursPercentage", ascending=False)

    total_row = {
        "instanceType": "Total",
        "CoverageHoursPercentage": total_df["CoverageHours"]["CoverageHoursPercentage"],
    }
    df.loc[len(df.index)] = total_row  # type: ignore
    df["CoverageHoursPercentage"] = utils.to_percentage(df["CoverageHoursPercentage"])
    return df


def get_reservation_coverage_df(
    client: CostExplorerClient,
    logger: Logger,
    service: str
) -> pd.DataFrame | None:
    reservation_coverage_data = get_reservation_coverage_data(client, logger, service)
    logger.debug("Reservation coverage data: %s", reservation_coverage_data)

    if reservation_coverage_data is None:
        return None
    if reservation_coverage_data["CoveragesByTime"][0]["Groups"] == []:
        logger.info("Reservation coverage data is empty")
        return None

    raw_reservation_coverage_df = reservation_coverage_to_df(reservation_coverage_data)
    logger.debug("Raw reservation coverage df: %s", raw_reservation_coverage_df)

    raw_total = reservation_coverage_data["CoveragesByTime"][0]["Total"]
    logger.debug("Raw total: %s", raw_total)
    return format_reservation_coverage_df(raw_reservation_coverage_df, raw_total)


#####################################################################################
#####################################################################################
# Reservations Purchase Recommendations


def get_reservations_purchase_recommendations_info(
    client: CostExplorerClient, input: list[dict], logger: Logger
) -> list[dict] | None:
    logger.info("Getting reservations purchase recommendations data")
    logger.debur("RPR_CONFIG: %s", RPR_CONFIG)
    rprs = []

    for rpr_input in RPR_CONFIG:
        try:
            rpr = client.get_reservation_purchase_recommendation(**rpr_input)[  # type: ignore
                "Recommendations"
            ]
            logger.debug("Reservations purchase recomendation raw data: %s", rpr)
        except client.exceptions.DataUnavailableException:
            logger.info(
                "Purchase recommendations data is unavailable for %s", rpr_input
            )
            continue
        except Exception as e:
            logger.warning(
                "Error while getting purchase recommendations for %s error: %s",
                rpr_input,
                e,
            )
            continue

        if rpr:
            rpr_dict = reservations_purchase_recomendations_to_dict(rpr[0], rpr_input)  # type: ignore
            if rpr_dict is not None:
                rprs.append(rpr_dict)

    return rprs


def reservations_purchase_recomendations_to_dict(
    recomendation: dict, rpr_input
) -> dict | None:
    if summary := recomendation.get("RecommendationSummary"):
        currency = summary.get("CurrencyCode")
        estimated_monthly_savings = summary.get("TotalEstimatedMonthlySavingsAmount")
        if estimated_monthly_savings is None:
            return
        estimated_monthly_savings = float(estimated_monthly_savings)
        lookback_period = recomendation["LookbackPeriodInDays"]

        return {
            "EstimatedMonthlySavings": estimated_monthly_savings,
            "Currency": currency,
            "LookbackPeriodInDays": lookback_period,
            "ServiceReservation": rpr_input["Service"],
        }


def format_reservations_purchase_recomendations_df(raw_df) -> pd.DataFrame:
    df = raw_df.copy()
    df["EstimatedMonthlySavings"] = utils.to_dollars(df["EstimatedMonthlySavings"])
    return df[
        [
            "ServiceReservation",
            "EstimatedMonthlySavings",
            "LookbackPeriodInDays",
        ]
    ]


def get_reservations_purchase_recommendations_df(
    client: CostExplorerClient,
    logger: Logger,
) -> pd.DataFrame | None:
    raw_rpr = get_reservations_purchase_recommendations_info(client, RPR_CONFIG, logger)

    if not raw_rpr:
        return None
    raw_rpr_df = pd.DataFrame(raw_rpr)
    return format_reservations_purchase_recomendations_df(raw_rpr_df)


def get_reservations_dataframes(
    ce_client: CostExplorerClient,
    logger: Logger,
    org_client
) -> list[dict] | None:
    if GET_RESERVED_INSTANCES_INFO:
        logger.info("Getting reservations dataframes")
        reservations_utilization_df = get_reservations_utilization_df(
            ce_client,
            logger,
            org_client
        )

        result = [
            {
                "Title": "Reservations utilization",
                "Dataframe": reservations_utilization_df
            },
            {
                "Title": "Reservation purchase recommendations",
                "Dataframe": get_reservations_purchase_recommendations_df(
                    ce_client, logger
                )
            },
        ]
        if reservations_utilization_df is not None:
            for s in LIST_OF_SERVICES_FOR_RESERVATIONS_COVERAGE:
                logger.info("Getting reservation coverage for %s", s)
                df = get_reservation_coverage_df(ce_client, logger, s)
                if df is not None:
                    result.append({"Title": f"{s} Reservation coverage", "Dataframe": df})
        
        return result
    logger.info("Getting reservations dataframes is disabled")
