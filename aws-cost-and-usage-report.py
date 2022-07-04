#!/usr/bin/env python3

import argparse
import boto3
import datetime
import pandas
import logging
import os
import copy

from calendar import monthrange
from dateutil.relativedelta import relativedelta

logging.basicConfig(format='%(levelname)s %(filename)s:%(lineno)s : %(message)s', level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_cost_and_usage(start_date, end_date, group_by=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}], granularity='MONTHLY', metrics=['UnblendedCost'], **kwargs):
    results = []
    token = None

    session = boto3.session.Session()
    ce = session.client('ce')
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
    df = df.append(row_wiht_total_series)

    # Sort data frame
    df.sort_values(df.columns.tolist(), ascending=False, inplace=True)

    final_df = df.copy()
    for column in df.columns:
        # column = 2021-05-01
        year = int(column.split('-')[0])
        month = int(column.split('-')[1])
        number_of_days = monthrange(year, month)[1]
        final_df[f'n {column}'] = df[column].div(number_of_days).round(2)

    # Insert separator between regular columns and normalized columns so it is easier
    # to write to file later on
    final_df.insert(3,'---','')

    logger.debug(f'Data frame with normalized data:\n{final_df}\n')

    return final_df


def get_cost_and_usage_report_per_service(top_five_services_by_max_diff, filter, metrics):
    top_five_services_df = {}
    for service_name in top_five_services_by_max_diff:
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
        top_five_df = ce_response_to_dataframe(result)
        top_five_df.sort_values(df.columns.tolist(), ascending=False, inplace=True)
        top_five_services_df[service_name] = {
            'df': top_five_df.copy(),
            'diff': df.loc[service_name][last_month_norm_column_name] - df.loc[service_name][month_before_last_norm_column_name]
            }
    return top_five_services_df


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description="Generate cost and usage report for the last 3 month grouped by service")
# pass sensitivity
parser.add_argument('--sensitivity', type=float, default=0.1, help="Sensitivity of cost change formatting")
parser.add_argument('--out', type=str, default=f'cost-and-usage-report-{datetime.date.today()}.xlsx', help="Output file name")
parser.add_argument('--debug', action="store_true", help="Print debug info")
parser.add_argument('--exclude_credit', action="store_true", default=True, help="Exclude credit from the report")
parser.add_argument('--exclude_refunds', action="store_true", default=True, help="Exclude refunds from the report")
args = parser.parse_args()

if args.debug:
    logger.setLevel(logging.DEBUG)

report_file_name = args.out
sensitivity = args.sensitivity
# 1st day of month 3 months ago
start = (datetime.date.today() - relativedelta(months=+3)).replace(day=1)
# the first day of the current month
end = datetime.date.today().replace(day=1)
filter = {"And": []}
if args.exclude_credit:
    filter['And'].append({'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Credit']}}})
if args.exclude_refunds:
    filter['And'].append({'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Refund']}}})
metrics=['UnblendedCost']
sts = boto3.client('sts')
account_id = sts.get_caller_identity().get('Account')
user_id = sts.get_caller_identity().get('Arn').split(':')[-1]

logger.info(f'Getting montly cost and usage report from {start} to {end}')
logger.info(f'Cost change sensitivity is set to {sensitivity}')
logger.info(f'Exclude credit {args.exclude_credit}')
logger.info(f'Exclude refunds {args.exclude_refunds}')

results = get_cost_and_usage(start, end, Filter=filter, metrics=metrics)

logger.debug(f'Response:\n{results}')

logger.info('Parsing report')

df = ce_response_to_dataframe(results)

logger.debug(f'Results converted to data frame:\n{df}\n')

logger.info('Calculating services with most differences and getting usage type break down for the top 5')
num_of_col = len(df.columns)
last_month_norm_column_name = df.columns[num_of_col - 1]
month_before_last_norm_column_name = df.columns[num_of_col - 2]
# This one get diff between last column and one before last and then returns indexes of rows with the max diff
rows_sorted_by_max_diff = (df[last_month_norm_column_name] - df[month_before_last_norm_column_name]).sort_values(ascending=False).index.values.tolist()
# Have to remove 'Total cost' row otherwise it will be always in the top 5
rows_sorted_by_max_diff.remove('Total Cost')
top_five_services_by_max_diff = rows_sorted_by_max_diff[0:5]
top_five_services_df = get_cost_and_usage_report_per_service(top_five_services_by_max_diff, filter, metrics=metrics)

# Get break down by account
logger.info('Preparing report grouped per account')
results_per_account = get_cost_and_usage(start, end, group_by=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}], granularity='MONTHLY', metrics=metrics, Filter=filter)
logger.debug(f'Response:\n{results_per_account}')
df_per_account = ce_response_to_dataframe(results_per_account)

logger.info(f'Writing repot to {report_file_name}')

if os.path.isfile(report_file_name):
    os.remove(report_file_name)

worksheet_name = 'Cost and usage report'
table_row_number = 6
sensitivity_value_cell = '$B$3'
normalized_cost_start_column_number = 5
normalized_cost_start_column_letter = chr(ord('@') + normalized_cost_start_column_number)
normalized_cost_end_column_letter = chr(ord('@') + len(df.columns) + 1)
comments_column_letter = chr(ord('@') + len(df.columns) + 2)
suggestions_column_letter = chr(ord('@') + len(df.columns) + 3)
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

    # Write top 5 services
    worksheet.write('A' + str(row_counter), 'Top 5 services break down by usage type')
    worksheet.merge_range(f'A{row_counter}:H{row_counter}', 'Top 5 services break down by usage type')
    row_counter += 1

    for service in top_five_services_by_max_diff:
        worksheet.merge_range(f'A{row_counter}:H{row_counter}', f'{service}. diff compared to prev month: {top_five_services_df[service]["diff"]}')
        top_five_services_df[service]['df'].to_excel(writer,
                                                sheet_name=worksheet_name,
                                                startrow=row_counter,
                                                startcol=0,
                                                index=True)
        row_counter = row_counter + len(top_five_services_df[service]['df'].index.values.tolist()) + 3

    # E1101: Instance of 'ExcelWriter' has no 'book' member (no-member)
    # pylint: disable=E1101

    text_column_format = workbook.add_format()
    text_column_format.set_text_wrap(True)
    text_column_format.set_align('left')
    # Set width and format of services columnt
    worksheet.set_column(0, 0, 23, text_column_format)
    # Set width of montly and daily cost columnts
    worksheet.set_column(1, 7, 12)
    # Set width of column that separates montly cost from daily cost to 5
    # to leave more space for comments and suggestions
    worksheet.set_column(4, 4, 5)
    # Set width and format of columns for suggestion and comments to 30
    worksheet.set_column(8, 9, 30, text_column_format)
    worksheet.merge_range(0, 0, 0, 9, 'Generated using https://github.com/fivexl/aws-cost-and-usage-report', merged_cell_format)
    worksheet.merge_range(1, 0, 1, 9, f'Generated by {user_id} for account {account_id} on {datetime.date.today()}', merged_cell_format)
    worksheet.merge_range(5, 0, 5, 3, 'Montly unblended cost per service', merged_cell_format)
    worksheet.merge_range(5, 5, 5, 7, 'Normalized values by number of days in the given month', merged_cell_format)
    worksheet.set_row(5, 30)
    worksheet.write('A3', 'Sensitivity')
    worksheet.write(sensitivity_value_cell, sensitivity)
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
