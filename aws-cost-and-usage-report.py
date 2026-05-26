#!/usr/bin/env python3

import argparse
from typing import Optional
import boto3
import datetime
import pandas
import logging
import os
import copy
import re
import urllib.request


from xlsxwriter.worksheet import Worksheet
from xlsxwriter.format import Format
import reservations
import savings_plans
from mypy_boto3_ce import CostExplorerClient
from logging import Logger

from calendar import monthrange
from dateutil.relativedelta import relativedelta

session = boto3.session.Session()
ce = boto3.client('ce')
sts = boto3.client('sts')
org_client = boto3.client("organizations")

logging.basicConfig(format='%(levelname)s %(filename)s:%(lineno)s : %(message)s', level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_cost_and_usage(start_date, end_date, group_by=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}], granularity='MONTHLY', metrics=['UnblendedCost'], **kwargs):
    results = []
    token = None

    logger.debug(f'get_cost_and_usage\nstart_date: {start_date}\nend_date: {end_date}\n' +
        f'group_by: {group_by}\ngranularity: {granularity}\nmetrics: {metrics}\nkwargs: {kwargs}')
    while True:
        if token:
            params = {'NextPageToken': token} + kwargs
        else:
            params = kwargs
        data = ce.get_cost_and_usage(
            TimePeriod={'Start': str(start_date), 'End':  str(end_date)},
            Granularity=granularity,
            Metrics=metrics,
            GroupBy=group_by,
            **params)
        results += data['ResultsByTime']
        token = data.get('NextPageToken')
        if not token:
            break

    return results


# Example result that we need to parse
# [{'TimePeriod': {'Start': '2021-05-01', 'End': '2021-06-01'},
#   'Total': {},
#   'Groups': [
#     {'Keys': ['AWS Backup'], 'Metrics': {'UnblendedCost': {'Amount': '0.6320089524', 'Unit': 'USD'}}},
#     {'Keys': ['AWS CloudTrail'], 'Metrics': {'UnblendedCost': {'Amount': '36.566997', 'Unit': 'USD'}}},
#     {'Keys': ['AWS Config'], 'Metrics': {'UnblendedCost': {'Amount': '238.304', 'Unit': 'USD'}}},
#     {'Keys': ['AWS Database Migration Service'], 'Metrics': {'UnblendedCost': {'Amount': '26.676219876', 'Unit': 'USD'}}}
# Above might change if query parameters are altered

# The first thing we do is we get the name of all services for all month
# We want to deal with the situation when we don't have a sevice for a first month but then it get added
# during the second or the third month. Thus we want to pre-populate those services with 0 since we are using
# append to form rows
def ce_response_to_dataframe(input):
    all_keys = []
    rows = {}
    column_names = []
    for month in input:
        for group in month['Groups']:
            key_name = group['Keys'][0]
            if key_name not in all_keys:
                all_keys.append(key_name)
    logger.debug(f'all_keys:\n{all_keys}')

    # Now when we know all the keys we need to deal with, we can start collecting data
    for month in input:
        column_names.append(month['TimePeriod']['Start'])
        all_keys_for_this_month = []
        for group in month['Groups']:
            key_name = group['Keys'][0]
            all_keys_for_this_month.append(key_name)
            if key_name not in rows:
                rows[key_name] = []
            rows[key_name].append(float(group['Metrics']['UnblendedCost']['Amount']))
        # now we need to add all keys that wasn't mentioned in this month but exist in
        # the final reports
        # first check that there are such keys
        if all_keys_for_this_month == all_keys:
            continue
        for key_name in all_keys:
            if key_name not in all_keys_for_this_month:
                if key_name not in rows:
                    rows[key_name] = []
                rows[key_name].append(float(0))

    df = pandas.DataFrame(rows.values(), columns=column_names, index=all_keys)
    df.fillna(value=0, inplace=True)
    df = df.round(2)
    logger.debug(f'Initial data frame:\n{df}\n')

    # drop all the rows with zeros since there could be quite many
    # after rounding
    df = df.loc[(df!=0).any(axis=1)]

    # calculate and append total cost. Note important that we do it before sorting
    row_with_total = []
    for column in df.columns:
        row_with_total.append(df[column].sum())
    row_wiht_total_series = pandas.Series(row_with_total, index = df.columns, name='Total Cost')
    df = pandas.concat([df, row_wiht_total_series.to_frame().T])

    # Sort data frame
    df.sort_values(by=df.columns[-1], ascending=False, inplace=True)

    final_df = df.copy()
    for column in df.columns:
        # column = 2021-05-01
        year = int(column.split('-')[0])
        month = int(column.split('-')[1])
        number_of_days = monthrange(year, month)[1]
        final_df[f'n {column}'] = df[column].div(number_of_days).round(2)

    # Insert separator between regular columns and normalized columns so it is easier
    # to write to file later on
    num_regular_columns = len(df.columns)
    final_df.insert(num_regular_columns, '---', '')

    logger.debug(f'Data frame with normalized data:\n{final_df}\n')

    return final_df


def get_cost_and_usage_report_per_service(top_n_services_by_max_diff, filter, metrics):
    top_n_services_df = {}
    for service_name in top_n_services_by_max_diff:
        service_filter = copy.deepcopy(filter)
        service_filter['And'].append(
            {
                "Dimensions": {
                    "Key": "SERVICE",
                    "Values": [service_name]
                }
            }
        )
        result = get_cost_and_usage(start, end, group_by=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE', }], Filter=service_filter, metrics=metrics)
        top_n_df = ce_response_to_dataframe(result)
        top_n_df.sort_values(df.columns.tolist(), ascending=False, inplace=True)
        top_n_services_df[service_name] = {
            'df': top_n_df.copy(),
            'diff': df.loc[service_name][last_month_norm_column_name] - df.loc[service_name][month_before_last_norm_column_name]
            }
    return top_n_services_df


def add_to_report(
        title: str,
        df: Optional[pandas.DataFrame],
        writer: pandas.ExcelWriter,
        worksheet: Worksheet,
        start_row: int,
        fmt: Format,
        WORKSHEET_NAME: str 
) -> int:
    if df is not None:
        df.to_excel(writer, sheet_name=WORKSHEET_NAME, startrow=start_row, startcol=0, index=False)
        worksheet.merge_range(start_row -1, 0, start_row -1, len(df.columns) -1, title, fmt)
        start_row += len(df.index) + 3
    else:
        worksheet.write(start_row, 0, f"No {title.lower()} info", fmt)
        start_row += 2
    return start_row

def add_savings_plans_info_to_report(
        start_row: int,
        client: CostExplorerClient,
        logger: Logger,
        merged_cell_format: Format,
        writer: pandas.ExcelWriter,
        worksheet: Worksheet,
        work_sheet_name: str
) -> int:
    savings_plans_dataframes = savings_plans.get_savings_plans_dataframes(client, logger, org_client)
    if savings_plans_dataframes is not None:
        for section_title, dfs in savings_plans_dataframes.items():
            worksheet.merge_range(start_row, 0, start_row, 9, section_title, merged_cell_format)
            start_row += 2
            for title, df in dfs.items():
                start_row = add_to_report(title, df, writer, worksheet, start_row, merged_cell_format, work_sheet_name)
    return start_row

            
def add_reservations_info_to_report(
        start_row: int,
        client: CostExplorerClient,
        logger: Logger,
        merged_cell_format: Format,
        writer: pandas.ExcelWriter,
        worksheet: Worksheet,
        work_sheet_name: str
) -> int:
    reservations_data = reservations.get_reservations_dataframes(client, logger, org_client)

    if reservations_data is not None:
        worksheet.merge_range(start_row, 0, start_row, 9, "Reservations Info", merged_cell_format)
        start_row += 2
        for data in reservations_data:
            title = data["Title"]
            df = data["Dataframe"]
            start_row = add_to_report(title, df, writer, worksheet, start_row, merged_cell_format, work_sheet_name)
    return start_row


def generate_llm_todo_list(
        top_services_by_max_diff: list,
        top_services_df: dict,
        df: pandas.DataFrame,
        output_file: str,
        last_month_norm_column_name: str,
        month_before_last_norm_column_name: str,
        start_date: datetime.date,
        end_date: datetime.date
) -> None:
    """Generate a plain text todo list for LLM research based on highest spend increases."""
    
    # Extract the actual month dates from the column names
    # Column names are like 'n 2025-10-01'
    last_month_date = last_month_norm_column_name.replace('n ', '')
    month_before_last_date = month_before_last_norm_column_name.replace('n ', '')
    
    with open(output_file, 'w') as f:
        # Header
        f.write("AWS Cost Research Todo List\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.date.today()}\n")
        f.write(f"Overall Analysis Period: {start_date} to {end_date}\n")
        f.write(f"Comparison Period: {month_before_last_date} vs {last_month_date}\n")
        f.write(f"Analysis based on normalized daily costs\n\n")
        
        f.write("Services to Research (ordered by highest spend increase):\n")
        f.write("=" * 80 + "\n\n")
        
        # Iterate through top services
        for idx, service_name in enumerate(top_services_by_max_diff, 1):
            service_info = top_services_df[service_name]
            current_cost = df.loc[service_name][last_month_norm_column_name]
            previous_cost = df.loc[service_name][month_before_last_norm_column_name]
            cost_diff = service_info['diff']
            
            # Calculate percentage increase
            if previous_cost > 0:
                percent_increase = (cost_diff / previous_cost) * 100
            else:
                percent_increase = 100 if cost_diff > 0 else 0
            
            # Write service header
            f.write(f"{idx}. {service_name}\n")
            f.write("-" * 80 + "\n")
            
            # Write cost details with explicit dates
            f.write(f"   Current normalized daily cost ({last_month_date}): ${current_cost:.2f}\n")
            f.write(f"   Previous normalized daily cost ({month_before_last_date}): ${previous_cost:.2f}\n")
            f.write(f"   Daily cost increase: ${cost_diff:.2f} ({percent_increase:+.1f}%)\n\n")
            
            # Write top usage types
            service_df = service_info['df']
            # Get the last month column (excluding normalized columns and separator)
            last_month_col = service_df.columns[2]  # Third column is the last month
            
            # Get top 10 usage types by cost in the last month
            top_usage_types = service_df.nlargest(10, last_month_col)
            
            if len(top_usage_types) > 0:
                f.write("   Top usage types contributing to cost:\n")
                for usage_idx, (usage_type, row) in enumerate(top_usage_types.iterrows(), 1):
                    if usage_type == 'Total Cost':
                        continue
                    usage_cost = row[last_month_col]
                    if usage_cost > 0:
                        f.write(f"     {usage_idx}. {usage_type}: ${usage_cost:.2f}\n")
                f.write("\n")
            
            # Write research tasks
            f.write("   Research tasks:\n")
            f.write("     * Investigate what changed in usage patterns between the two periods\n")
            f.write("     * Check for any new deployments, scaling events, or configuration changes\n")
            f.write("     * Review if this increase is expected (e.g., planned growth) or anomalous\n")
            f.write("     * Identify opportunities for cost optimization (rightsizing, reserved capacity, etc.)\n")
            f.write("     * Verify that usage aligns with business requirements and expected workload\n")
            f.write("\n\n")
        
        # Footer
        f.write("=" * 80 + "\n")
        f.write("End of Research Todo List\n")
    
    logger.info(f'Todo list for LLM research written to {output_file}')


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description="Generate cost and usage report for the last N months grouped by service")
# pass sensitivity
parser.add_argument('--months', type=int, default=3, help="Number of months to include in the report (will use maximum available if requested months exceed available data)")
parser.add_argument('--sensitivity', type=float, default=0.1, help="Sensitivity of cost change formatting")
parser.add_argument('--out', type=str, default=f'cost-and-usage-report-{datetime.date.today()}.xlsx', help="Output file name")
parser.add_argument('--debug', action="store_true", help="Print debug info")
parser.add_argument('--exclude_credit', action="store_true", default=True, help="Exclude credit from the report")
parser.add_argument('--exclude_refunds', action="store_true", default=True, help="Exclude refunds from the report")
parser.add_argument('--top_n', type=int, default=10, help="Number of top services by spend increase to analyze")
parser.add_argument('--todo_output', type=str, default=f'cost-research-todos-{datetime.date.today()}.txt', help="Output file name for LLM research todo list")
parser.add_argument('--download_invoices', action="store_true", default=False, help="Download invoice PDFs for each billing period scanned into an 'invoices' folder")
args = parser.parse_args()

if args.debug:
    logger.setLevel(logging.DEBUG)

report_file_name = args.out
sensitivity = args.sensitivity
num_months = args.months
if num_months < 1:
    logger.error('Number of months must be at least 1')
    raise SystemExit(1)
# 1st day of month N months ago
start = (datetime.date.today() - relativedelta(months=+num_months)).replace(day=1)
# the first day of the current month
end = datetime.date.today().replace(day=1)
filter = {"And": []}
if args.exclude_credit:
    filter['And'].append({'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Credit']}}})
if args.exclude_refunds:
    filter['And'].append({'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Refund']}}})
metrics=['UnblendedCost']
account_id = sts.get_caller_identity().get('Account')
user_id = sts.get_caller_identity().get('Arn').split(':')[-1]

logger.info(f'Getting monthly cost and usage report from {start} to {end} ({num_months} months requested)')
logger.info(f'Cost change sensitivity is set to {sensitivity}')
logger.info(f'Exclude credit {args.exclude_credit}')
logger.info(f'Exclude refunds {args.exclude_refunds}')

results = get_cost_and_usage(start, end, Filter=filter, metrics=metrics)

# If fewer months were returned than requested, log a warning
actual_months = len(results)
if actual_months < num_months:
    logger.warning(f'Requested {num_months} months but only {actual_months} months of data available. Using maximum available.')

logger.debug(f'Response:\n{results}')

logger.info('Parsing report')

df = ce_response_to_dataframe(results)

logger.debug(f'Results converted to data frame:\n{df}\n')

logger.info(f'Calculating services with most differences and getting usage type break down for the top {args.top_n}')
num_of_col = len(df.columns)
last_month_norm_column_name = df.columns[num_of_col - 1]
month_before_last_norm_column_name = df.columns[num_of_col - 2]
# This one get diff between last column and one before last and then returns indexes of rows with the max diff
rows_sorted_by_max_diff = (df[last_month_norm_column_name] - df[month_before_last_norm_column_name]).sort_values(ascending=False).index.values.tolist()
# Have to remove 'Total cost' row otherwise it will be always in the top N
rows_sorted_by_max_diff.remove('Total Cost')
top_n_services_by_max_diff = rows_sorted_by_max_diff[0:args.top_n]
top_n_services_df = get_cost_and_usage_report_per_service(top_n_services_by_max_diff, filter, metrics=metrics)

# Generate LLM research todo list
generate_llm_todo_list(
    top_services_by_max_diff=top_n_services_by_max_diff,
    top_services_df=top_n_services_df,
    df=df,
    output_file=args.todo_output,
    last_month_norm_column_name=last_month_norm_column_name,
    month_before_last_norm_column_name=month_before_last_norm_column_name,
    start_date=start,
    end_date=end
)

# Get break down by account
logger.info('Preparing report grouped per account')
results_per_account = get_cost_and_usage(start, end, group_by=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}], granularity='MONTHLY', metrics=metrics, Filter=filter)
logger.debug(f'Response:\n{results_per_account}')


def get_account_name(account_id, organization_client):
    response = organization_client.describe_account(AccountId=account_id)
    return response['Account']['Name']


def get_account_name_for_account_id_index(
        df_per_account: pandas.DataFrame,
        organization_client,
)-> pandas.DataFrame:
    # Iterate over the index of the dataframe
    for i in range(len(df_per_account.index)):
        idx = df_per_account.index[i]

        # If the index is an AWS Account ID, replace it with the account name
        if re.match(r'\d{12}', idx):  # regex to check if the string looks like an AWS Account ID # type: ignore
            try:
                account_name = get_account_name(idx, organization_client=organization_client)
                account_info = f"{account_name}({idx})"
                df_per_account = df_per_account.rename(index={idx: account_info})
            except Exception as e:
                print(f'Failed to get account name for id {idx}. Error: {e}')
                continue
    return df_per_account

df_per_account = ce_response_to_dataframe(results_per_account)
df_per_account = get_account_name_for_account_id_index(df_per_account, org_client)

logger.info(f'Writing repot to {report_file_name}')

if os.path.isfile(report_file_name):
    os.remove(report_file_name)

def col_num_to_excel_letter(col_num):
    """Convert a 1-based column number to Excel column letter (1=A, 26=Z, 27=AA, etc.)."""
    result = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result

worksheet_name = 'Cost and usage report'
table_row_number = 6
sensitivity_value_cell = '$B$3'
num_data_columns = len(df.columns)  # includes monthly costs + separator + normalized costs
num_monthly_columns = (num_data_columns - 1) // 2  # subtract separator, divide by 2 (monthly + normalized)
normalized_cost_start_column_number = num_monthly_columns + 2  # +1 for index col, +1 for separator
normalized_cost_start_column_letter = col_num_to_excel_letter(normalized_cost_start_column_number)
normalized_cost_end_column_letter = col_num_to_excel_letter(len(df.columns) + 1)
comments_column_letter = col_num_to_excel_letter(len(df.columns) + 2)
suggestions_column_letter = col_num_to_excel_letter(len(df.columns) + 3)
# E0110: Abstract class 'ExcelWriter' with abstract methods instantiated (abstract-class-instantiated)
# pylint: disable=E0110
with pandas.ExcelWriter(report_file_name, engine='xlsxwriter') as writer:

    # Write primary report
    df.to_excel(writer,
                sheet_name=worksheet_name,
                startrow=table_row_number,
                startcol=0,
                index=True)
    row_counter = table_row_number + len(df.index.values.tolist()) + 3

    workbook = writer.book
    worksheet = writer.sheets[worksheet_name]
    merged_cell_format = workbook.add_format()
    merged_cell_format.set_text_wrap(True)
    merged_cell_format.set_align('center')
    merged_cell_format.set_align('top')

    # Write per account break down
    worksheet.merge_range(f'A{row_counter}:H{row_counter}', f'Report grouped per linked account')
    df_per_account.to_excel(writer,
        sheet_name=worksheet_name,
        startrow=row_counter,
        startcol=0,
        index=True
    )
    row_counter += len(df_per_account.index.values.tolist()) + 2

    # Write top N services
    worksheet.write('A' + str(row_counter), f'Top {args.top_n} services break down by usage type')
    worksheet.merge_range(f'A{row_counter}:H{row_counter}', f'Top {args.top_n} services break down by usage type')
    row_counter += 1

    for service in top_n_services_by_max_diff:
        worksheet.merge_range(f'A{row_counter}:H{row_counter}', 
                     f'{service}. diff compared to prev month: {top_n_services_df[service]["diff"]:.2f}')
        top_n_services_df[service]['df'].to_excel(writer,
                                                sheet_name=worksheet_name,
                                                startrow=row_counter,
                                                startcol=0,
                                                index=True)
        row_counter = row_counter + len(top_n_services_df[service]['df'].index.values.tolist()) + 3

    # Write Savings Plans info
    row_counter = add_savings_plans_info_to_report(
        start_row = row_counter,
        client = ce,
        logger = logger,
        merged_cell_format = merged_cell_format,
        writer = writer,
        worksheet = worksheet,
        work_sheet_name = worksheet_name
    )

    # Write Reservations info
    row_counter = add_reservations_info_to_report(
        start_row = row_counter,
        client = ce,
        logger = logger,
        merged_cell_format = merged_cell_format,
        writer = writer,
        worksheet = worksheet,
        work_sheet_name = worksheet_name
    )

    # E1101: Instance of 'ExcelWriter' has no 'book' member (no-member)
    # pylint: disable=E1101

    text_column_format = workbook.add_format()
    text_column_format.set_text_wrap(True)
    text_column_format.set_align('left')
    # Set width and format of services column
    worksheet.set_column(0, 0, 23, text_column_format)
    # Set width of monthly and daily cost columns
    separator_col = num_monthly_columns + 1  # +1 for index column
    last_data_col = len(df.columns)
    worksheet.set_column(1, last_data_col, 12)
    # Set width of column that separates monthly cost from daily cost to 5
    worksheet.set_column(separator_col, separator_col, 5)
    # Set width and format of columns for suggestion and comments to 30
    worksheet.set_column(last_data_col + 1, last_data_col + 2, 30, text_column_format)
    worksheet.merge_range(0, 0, 0, last_data_col + 2, 'Generated using https://github.com/fivexl/aws-cost-and-usage-report', merged_cell_format)
    worksheet.merge_range(1, 0, 1, last_data_col + 2, f'Generated by {user_id} for account {account_id} on {datetime.date.today()}', merged_cell_format)
    worksheet.merge_range(5, 0, 5, num_monthly_columns, 'Montly unblended cost per service', merged_cell_format)
    worksheet.merge_range(5, separator_col + 1, 5, last_data_col, 'Normalized values by number of days in the given month', merged_cell_format)
    worksheet.set_row(5, 30)
    worksheet.write('A3', 'Sensitivity')
    worksheet.write(sensitivity_value_cell, sensitivity)
    worksheet.write('A4', 'Credit excluded')
    worksheet.write('B4', 'Yes' if args.exclude_credit else 'No')
    worksheet.write('A5', 'Refund excluded')
    worksheet.write('B5', 'Yes' if args.exclude_refunds else 'No')
    worksheet.write(f'{comments_column_letter}{table_row_number + 1}', 'Comments', merged_cell_format)
    worksheet.write(f'{suggestions_column_letter}{table_row_number + 1}', 'Suggestions', merged_cell_format)

    red_background = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#000000'})
    green_background = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#000000'})
    white_background = workbook.add_format({'bg_color': '#FFFFFF', 'font_color': '#000000'})

    # if previous month value is empty then do nothing
    worksheet.conditional_format(
        f'{normalized_cost_start_column_letter}{table_row_number+2}:{normalized_cost_end_column_letter}1000',
        {
            'type': 'formula',
            'criteria': '=ISBLANK(INDIRECT(ADDRESS(ROW(), COLUMN()-1)))',
            'format': white_background
        }
    )

    # if current month value is empty then do nothing
    worksheet.conditional_format(
        f'{normalized_cost_start_column_letter}{table_row_number+2}:{normalized_cost_end_column_letter}1000',
        {
            'type': 'formula',
            'criteria': '=ISBLANK(INDIRECT(ADDRESS(ROW(), COLUMN())))',
            'format': white_background
        }
    )

    # current month value - prev month value >= sensitivity factor, i.e. cost is more than its been
    worksheet.conditional_format(
        f'{normalized_cost_start_column_letter}{table_row_number+2}:{normalized_cost_end_column_letter}1000',
        {
            'type': 'formula',
            'criteria': f'=(INDIRECT(ADDRESS(ROW(), COLUMN())) - INDIRECT(ADDRESS(ROW(), COLUMN()-1))) >= INDIRECT("{sensitivity_value_cell}")',
            'format': red_background
        }
    )

    # current month value - prev month value < sensitivity factor, i.e. cost is less than its been
    worksheet.conditional_format(
        f'{normalized_cost_start_column_letter}{table_row_number+2}:{normalized_cost_end_column_letter}1000',
        {
            'type': 'formula',
            'criteria': f'=(INDIRECT(ADDRESS(ROW(), COLUMN())) - INDIRECT(ADDRESS(ROW(), COLUMN()-1))) < (-1)*INDIRECT("{sensitivity_value_cell}")',
            'format': green_background
        }
    )

logger.info('Done')


# Download invoices if requested
if args.download_invoices:
    logger.info('Downloading invoice PDFs...')
    invoicing_client = boto3.client('invoicing', region_name='us-east-1')
    invoices_dir = f'invoices-{datetime.date.today()}'
    os.makedirs(invoices_dir, exist_ok=True)

    # Iterate over each month in the scanned period
    current_date = start
    while current_date < end:
        year = current_date.year
        month = current_date.month
        logger.info(f'Fetching invoices for {year}-{month:02d}...')

        try:
            # List invoice summaries for this billing period
            invoice_summaries = []
            next_token = None
            while True:
                params = {
                    'Selector': {
                        'ResourceType': 'ACCOUNT_ID',
                        'Value': account_id
                    },
                    'Filter': {
                        'BillingPeriod': {
                            'Month': month,
                            'Year': year
                        }
                    }
                }
                if next_token:
                    params['NextToken'] = next_token

                response = invoicing_client.list_invoice_summaries(**params)
                invoice_summaries.extend(response.get('InvoiceSummaries', []))
                next_token = response.get('NextToken')
                if not next_token:
                    break

            if not invoice_summaries:
                logger.info(f'  No invoices found for {year}-{month:02d}')
            else:
                logger.info(f'  Found {len(invoice_summaries)} invoice(s) for {year}-{month:02d}')

            for invoice in invoice_summaries:
                invoice_id = invoice['InvoiceId']
                billing_period = f"{year}-{month:02d}"
                try:
                    pdf_response = invoicing_client.get_invoice_pdf(InvoiceId=invoice_id)
                    invoice_pdf = pdf_response.get('InvoicePDF', {})
                    document_url = invoice_pdf.get('DocumentUrl')

                    if document_url:
                        filename = f"{billing_period}_{invoice_id}.pdf"
                        filepath = os.path.join(invoices_dir, filename)
                        urllib.request.urlretrieve(document_url, filepath)
                        logger.info(f'  Downloaded invoice for {month:02d}.{year}')

                    # Also download supplemental documents if any
                    for supp_doc in invoice_pdf.get('SupplementalDocuments', []):
                        supp_url = supp_doc.get('DocumentUrl')
                        supp_type = supp_doc.get('DocumentType', 'SUPPLEMENT')
                        supp_id = supp_doc.get('DocumentId', 'unknown')
                        if supp_url:
                            supp_filename = f"{billing_period}_{invoice_id}_{supp_type}_{supp_id}.pdf"
                            supp_filepath = os.path.join(invoices_dir, supp_filename)
                            urllib.request.urlretrieve(supp_url, supp_filepath)
                            logger.info(f'  Downloaded supplemental document for {month:02d}.{year}')

                except Exception as e:
                    logger.warning(f'  Failed to download invoice {invoice_id}: {e}')

        except Exception as e:
            logger.warning(f'  Failed to list invoices for {year}-{month:02d}: {e}')

        # Move to next month
        current_date = (current_date + relativedelta(months=+1)).replace(day=1)

    logger.info(f'Invoice PDFs saved to {invoices_dir}/')
