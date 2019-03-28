# React / Google BigQuery Dashboard for Analysis of Stock Market Events

### Use Google Cloud Serverless components to create a "real-time" dashboard to analyze daily stock prices for key events

-----

### Problem

The stock market is difficult to interpret, but some key events have been know as good predictors of future performance. This project is designed to show a method for analyzing stock events in "real time" using a serverless solution.

--- 

#### Data Flow

- Download from Alpha Vantage
- Store In BigQuery
- Process Time Series Statistics
- Quary TS Stats for Events
- Store New Events in `events` Table
- Cloud Function API polls Events Table and Stock History
- Display in Dashboard

![Screenshot](/images/data_flow.png)


#### Dashboard

![Screenshot](/images/dashboard.png)