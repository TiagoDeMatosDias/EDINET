# How to Run the Application

This application's execution flow is controlled by the `config/run_config.json` file. You can enable or disable specific parts of the application by editing this file.

## Configuration

The `config/run_config.json` file contains a JSON object with a single key, `run_steps`. The value of this key is another JSON object that contains a list of boolean flags for each step of the application.

To enable a step, set its value to `true`. To disable a step, set its value to `false`.

### Example `run_config.json`

```json
{
  "run_steps": {
    "get_documents": true,
    "download_documents": true,
    "standardize_data": false,
    "generate_financial_ratios": false,
    "aggregate_ratios": false,
    "update_stock_prices": true
  }
}
```

In this example, the application will:

1.  Get all documents with metadata.
2.  Download the documents.
3.  Skip standardizing data.
4.  Skip generating financial ratios.
5.  Skip aggregating ratios.
6.  Update stock prices.

## Steps

- `get_documents`: Fetches the list of available documents from the EDINET API.
- `download_documents`: Downloads the documents that match the specified criteria.
- `standardize_data`: Standardizes the downloaded financial data.
- `generate_financial_ratios`: Generates financial ratios from the standardized data.
- `aggregate_ratios`: Aggregates the generated financial ratios.
- `update_stock_prices`: Updates stock prices from Yahoo Finance.
