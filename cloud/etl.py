import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import json
import os
from google.cloud import bigquery


"""
gcp commands:
get schema:  bq show --schema ticker_test_120718.stocks > stocks_schema.json
"""

DIR = os.path.dirname(os.path.realpath(__file__))


def download_stock(symbol, out_path):

    print 'Downloading', symbol
    out_fname = os.path.join(out_path, 'daily-'+symbol+'.csv')

    _api_key = str(json.load(open('secrets.json', 'rb'))['alpha-vantage-api-key'])
    _ts = TimeSeries(key=_api_key, output_format='pandas')
    
    data, meta_data = _ts.get_daily_adjusted(symbol=symbol, outputsize='full')
    data.index = pd.to_datetime(data.index)
    data['date'] = data.index
    column_map = {
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. adjusted close': 'adj_close',
        '6. volume': 'volume',
        '7. dividend amount': 'dividend_amount',
        '8. split coefficient': 'split_coefficient'
    }

    for from_key, to_key in column_map.items():
        data[to_key] = data[from_key]
        del data[from_key]

    data['symbol'] = symbol


    cols = get_columns()
    data[cols].to_csv(out_fname, index=False)


def get_columns():
    return [col['name'] for col in json.load(open(os.path.join(DIR, 'stocks_schema.json')))]


def delete_from_table(symbol, table):
    client = bigquery.Client()
    query_job = client.query("""
        DELETE 
        FROM `%s` 
        WHERE symbol="%s" 
        """ % (table, symbol)
     )
    results = query_job.result()
    print('Deleted Rows for symbol %s, from table %s'%(symbol,table))


def delete_data(symbol):
    delete_from_table(symbol, 'ticker-224822.ticker_test_120718.stocks')

def upload_file(symbol):
    """
     bq load --noreplace --source_format=CSV --skip_leading_rows=1 ticker_test_120718.stocks ./stage_upload_to_gcs/daily-DIS.csv
    """
    client = bigquery.Client()
    filename = os.path.join(DIR, 'stage_upload_to_gcs/daily-%s.csv') % (symbol,)
    dataset_id = 'ticker_test_120718'
    table_id = 'stocks'

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    # job_config.autodetect = True

    with open(filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

def update_analytics_output(full_refresh=False):

    """
        TODO: this can be done similar to how you're handling update_technical_events()
        by joining first and then updating any new records.
    """
    if full_refresh:
        client = bigquery.Client()
        delete = """
        DELETE
        FROM `ticker-224822.ticker_test_120718.analytics_output`
        WHERE 
            1=1
        """
        query_job = client.query(delete)
        results = query_job.result()
        print("deleted all rows in ", "`ticker-224822.ticker_test_120718.analytics_output`")


    qry = """
        insert into `ticker-224822.ticker_test_120718.analytics_output`(
            date	,
            adj_close	,
            symbol	,
            sma_20	,
            std_20	,
            sma_50	,
            sma_200	,
            bb_perc_20	,
            adj_close_lag_1	,
            adj_close_lag_5	,
            adj_close_lag_10	,
            adj_close_lag_20	,
            adj_close_lead_1	,
            adj_close_lead_5	,
            adj_close_lead_10	,
            adj_close_lead_20	,
            min_lag_20	,
            max_lag_20	,
            mean_lag_20	,
            std_lag_20	,
            min_lag_50	,
            max_lag_50	,
            mean_lag_50	,
            std_lag_50	,
            min_lag_200	,
            max_lag_200	,
            mean_lag_200	,
            std_lag_200	,
            min_lead_20	,
            max_lead_20	,
            mean_lead_20	,
            std_lead_20	,
            min_lead_50	,
            max_lead_50	,
            mean_lead_50	,
            std_lead_50	,
            min_lead_200	,
            max_lead_200	,
            mean_lead_200	,
            std_lead_200
        )
        select
            vw.date,
            vw.adj_close,
            vw.symbol,
            vw.sma_20,
            vw.std_20,
            vw.sma_50,
            vw.sma_200,
            vw.bb_perc_20,
            vw.adj_close_lag_1,
            vw.adj_close_lag_5,
            vw.adj_close_lag_10,
            vw.adj_close_lag_20,
            vw.adj_close_lead_1,
            vw.adj_close_lead_5,
            vw.adj_close_lead_10,
            vw.adj_close_lead_20,
            vw.min_lag_20,
            vw.max_lag_20,
            vw.mean_lag_20,
            vw.std_lag_20,
            vw.min_lag_50,
            vw.max_lag_50,
            vw.mean_lag_50,
            vw.std_lag_50,
            vw.min_lag_200,
            vw.max_lag_200,
            vw.mean_lag_200,
            vw.std_lag_200,
            vw.min_lead_20,
            vw.max_lead_20,
            vw.mean_lead_20,
            vw.std_lead_20,
            vw.min_lead_50,
            vw.max_lead_50,
            vw.mean_lead_50,
            vw.std_lead_50,
            vw.min_lead_200,
            vw.max_lead_200,
            vw.mean_lead_200,
            vw.std_lead_200
        FROM `ticker-224822.ticker_test_120718.analytics_view` as vw
        left join `ticker-224822.ticker_test_120718.analytics_output` as output 
        on vw.date = output.date
        and vw.symbol = output.symbol
        where
            output.date is null
    """
    client = bigquery.Client()
    query_job = client.query(qry)
    results = query_job.result()
    print("updated analytics", results)

def update_technical_events():
    qry = """
    insert into `ticker-224822.ticker_test_120718.events`(
        date,
        symbol,
        event
    )
    SELECT 
        cross_event.date,
        cross_event.symbol,
        cross_event.event
    FROM `ticker-224822.ticker_test_120718.analytics_cross_event` as cross_event
    left join `ticker-224822.ticker_test_120718.events` as events
    on cross_event.date = events.date
    and cross_event.symbol = events.symbol
    where
        events.date is null
    """
    client = bigquery.Client()
    query_job = client.query(qry)
    results = query_job.result()
    print("update_technical_events", results)



def refresh_and_update_stock(symbol, csv_stage_path="./stage_upload_to_gcs"):
    download_stock(symbol, csv_stage_path)
    delete_data(symbol)
    upload_file(symbol)

def main():
    for symbol in ['AAPL', 'DIS', 'SPY', 'GOOG', 'XOM', 'NFLX']:
        refresh_and_update_stock(symbol)
 
    update_analytics_output(full_refresh=False)
    update_technical_events()


if __name__ == "__main__":
    main()