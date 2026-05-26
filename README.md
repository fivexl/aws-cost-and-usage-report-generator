[![FivexL](https://releases.fivexl.io/fivexlbannergit.jpg)](https://fivexl.io/)

# aws-cost-and-usage-report-generator

Python script to generate montly AWS cost and usage report break down

## Requierments 

UV 
```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Usage

Script will generate monthly unblended cost report for a configurable number of months as well as calculate normalized daily cost.
By default it fetches the last 3 months. You can specify a different number with the `--months` parameter — if the requested number exceeds available data, the script will use the maximum available.

Make sure to set AWS creds for the account for which you want to generate a report.

Take the output file, import it to Google Sheets, fill in comments and suggestions, export as pdf and share with the customer.
Output file is optimized for the workflow above.

```
> bash setup.sh
....

> source .venv/bin/activate

> uv run aws-cost-and-usage-report.py -h
usage: aws-cost-and-usage-report.py [-h] [--months MONTHS] [--sensitivity SENSITIVITY] [--out OUT] [--debug]
                                    [--exclude_credit] [--exclude_refunds] [--top_n TOP_N] [--todo_output TODO_OUTPUT]

Generate cost and usage report for the last N months grouped by service

optional arguments:
  -h, --help            show this help message and exit
  --months MONTHS       Number of months to include in the report (will use maximum available
                        if requested months exceed available data) (default: 3)
  --sensitivity SENSITIVITY
                        Sensitivity of cost change formatting (default: 0.1)
  --out OUT             Output file name (default: cost-and-usage-report-<today>.xlsx)
  --debug               Print debug info (default: False)
  --exclude_credit      Exclude credit from the report (default: True)
  --exclude_refunds     Exclude refunds from the report (default: True)
  --top_n TOP_N         Number of top services by spend increase to analyze (default: 10)
  --todo_output TODO_OUTPUT
                        Output file name for LLM research todo list (default: cost-research-todos-<today>.txt)

# Default (last 3 months)
> python3 aws-cost-and-usage-report.py

# Last 6 months
> python3 aws-cost-and-usage-report.py --months 6

# Last 12 months (or max available)
> python3 aws-cost-and-usage-report.py --months 12
```
## Example

See [example report](cost-and-usage-report-2021-08-05.xlsx) for more details

## Post review

- Post review [url](https://github.com/fivexl/aws-cost-and-usage-report-generator/compare/review...main)
