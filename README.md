[![FivexL](https://releases.fivexl.io/fivexlbannergit.jpg)](https://fivexl.io/)

# aws-cost-and-usage-report-generator

Python script to generate montly AWS cost and usage report break down

## Usage

Script will generate montly unblended cost report for the last 3 month as well will calculate normalized daily cost.
Make sure to set AWS creds for the account for which you want to generate a report

```
> bash setup.sh
....

> source env/bin/activate

> python3 aws-cost-and-usage-report.py -h
usage: aws-cost-and-usage-report.py [-h] [--sensetivity SENSETIVITY] [--out OUT] [--debug]

Generate cost and usage report for the last 3 month grouped by service

optional arguments:
  -h, --help            show this help message and exit
  --sensetivity SENSETIVITY
                        Sensetivity of cost change formatting (default: 0.1)
  --out OUT             Output file name (default: cost-and-usage-report-2021-08-05.xlsx)
  --debug               Print debug info (default: False)

> python3 aws-cost-and-usage-report.py 
INFO: Found credentials in environment variables.
INFO: Getting montly cost and usage report from 2021-05-01 to 2021-08-01
INFO: Cost change sensetivity is set to 0.1
INFO: Found credentials in environment variables.
INFO: Parsing report
INFO: Calculating total cost per month
INFO: Calculating normalized cost per month
INFO: Writing repot to cost-and-usage-report-2021-08-05.xlsx
INFO: Done
```
## Example

See [example report](cost-and-usage-report-2021-08-05.xlsx) for more details