#!/usr/bin/env python3

import argparse
import boto3
import datetime
import pandas
import logging
import os

from calendar import monthrange
from dateutil.relativedelta import relativedelta

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description="Generate cost and usage report for the last 3 month grouped by service")
# pass sensitivity
parser.add_argument('--sensitivity', type=float, default=0.1, help="Sensitivity of cost change formatting")
parser.add_argument('--out', type=str, default=f'cost-and-usage-report-{datetime.date.today()}.xlsx', help="Output file name")
parser.add_argument('--debug', action="store_true", help="Print debug info")
args = parser.parse_args()

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
if args.debug:
    logging.root.setLevel(logging.DEBUG)

report_file_name = args.out
sensitivity = args.sensitivity
# 1st day of month 3 months ago
start = (datetime.date.today() - relativedelta(months=+3)).replace(day=1)
# the first day of the current month
end = datetime.date.today().replace(day=1)
sts = boto3.client('sts')
account_id = sts.get_caller_identity().get('Account')
user_id = sts.get_caller_identity().get('Arn').split(':')[-1]

logging.info(f'Getting montly cost and usage report from {start} to {end}')
logging.info(f'Cost change sensitivity is set to {sensitivity}')

session = boto3.session.Session()
cd = session.client('ce')

results = []

token = None
while True:
    if token:
        kwargs = {'NextPageToken': token}
    else:
        kwargs = {}
    data = cd.get_cost_and_usage(
        TimePeriod={'Start': str(start), 'End':  str(end)},
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
        **kwargs)
    results += data['ResultsByTime']
    token = data.get('NextPageToken')
    if not token:
        break

logging.debug(f'Response:\n{results}')

logging.info('Parsing report')

rows = {}
column_names = ['Service']

# Example result that we need to parse
# [{'TimePeriod': {'Start': '2021-05-01', 'End': '2021-06-01'},
#   'Total': {},
#   'Groups': [
#     {'Keys': ['AWS Backup'], 'Metrics': {'UnblendedCost': {'Amount': '0.6320089524', 'Unit': 'USD'}}},
#     {'Keys': ['AWS CloudTrail'], 'Metrics': {'UnblendedCost': {'Amount': '36.566997', 'Unit': 'USD'}}},
#     {'Keys': ['AWS Config'], 'Metrics': {'UnblendedCost': {'Amount': '238.304', 'Unit': 'USD'}}},
#     {'Keys': ['AWS Database Migration Service'], 'Metrics': {'UnblendedCost': {'Amount': '26.676219876', 'Unit': 'USD'}}}
# Above might change if query parameters are altered
for month in results:
    column_names.append(month['TimePeriod']['Start'])
    for service in month['Groups']:
        service_name = service['Keys'][0]
        if service_name not in rows:
            rows[service_name] = [service_name]
        rows[service_name].append(float(service['Metrics']['UnblendedCost']['Amount']))

df = pandas.DataFrame(rows.values(), columns=column_names)
df.fillna(value=0, inplace=True)
df = df.round(2)

logging.debug(f'Results converted to data frame:\n{df}\n')

logging.info('Calculating total cost per month')

# calculate total cost per month
row_with_total = ['Total cost']
for column in column_names[1:]:
    row_with_total.append(df[column].sum())

df.loc[len(df.index)] = row_with_total

logging.debug(f'Data frame with added totals:\n{df}\n')

df = df.sort_values(column_names[-1], ascending=False)

logging.debug(f'Sorted data frame with added totals:\n{df}\n')

logging.info('Calculating normalized cost per month')

normalized_df = df.copy()
for column in column_names[1:]:
    # column = 2021-05-01
    year = int(column.split('-')[0])
    month = int(column.split('-')[1])
    number_of_days = monthrange(year, month)[1]
    normalized_df[column] = normalized_df[column].div(number_of_days).round(2)

logging.debug(f'Normalized by number of days in a month data frame:\n{normalized_df}\n')

logging.info(f'Writing repot to {report_file_name}')

if os.path.isfile(report_file_name):
    os.remove(report_file_name)

worksheet_name = 'Cost and usage report'
table_row_number = 6
sensitivity_value_cell = '$B$3'
normalized_cost_start_column_number = 5
normalized_cost_start_column_letter = chr(ord('@') + normalized_cost_start_column_number + 1)
normalized_cost_end_column_letter = chr(ord('@') + normalized_cost_start_column_number + len(column_names) - 1)
comments_column_letter = chr(ord('@') + normalized_cost_start_column_number + len(column_names))
suggestions_column_letter = chr(ord('@') + normalized_cost_start_column_number + len(column_names) + 1)
# E0110: Abstract class 'ExcelWriter' with abstract methods instantiated (abstract-class-instantiated)
# pylint: disable=E0110
with pandas.ExcelWriter(report_file_name, engine='xlsxwriter') as writer:
    df.to_excel(writer,
                sheet_name=worksheet_name,
                startrow=table_row_number,
                startcol=0,
                index=False)
    normalized_df.loc[:, df.columns != 'Service'].to_excel(writer,
                                                           sheet_name=worksheet_name,
                                                           startrow=table_row_number,
                                                           startcol=normalized_cost_start_column_number,
                                                           index=False)

    # E1101: Instance of 'ExcelWriter' has no 'book' member (no-member)
    # pylint: disable=E1101
    workbook = writer.book
    worksheet = writer.sheets[worksheet_name]
    merged_cell_format = workbook.add_format()
    merged_cell_format.set_text_wrap()
    merged_cell_format.set_align('center')
    merged_cell_format.set_align('top')
    worksheet.set_column(0, 0, 30)
    worksheet.set_column(1, 7, 12)
    worksheet.set_column(8, 9, 30)
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

logging.info('Done')
