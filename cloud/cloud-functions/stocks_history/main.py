from google.cloud import bigquery
import datetime
import json
from flask import jsonify, Response



def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def custom_jsonify(obj):
    return Response(json.dumps(obj, default=default), status=200, mimetype='application/json')

def stocks_history(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    symbol = request.args.get('symbol')

    if symbol is None:
        return jsonify([])

    client = bigquery.Client()
    qry = client.query("""
    SELECT 
        date,
        adj_close,
        symbol,
        sma_20,
        std_20,
        sma_50,
        sma_200,
        bb_perc_20
    FROM `ticker-224822.ticker_test_120718.analytics_view`
    where 
      symbol = '{symbol}'
    and extract(year from date) >= 2010
    """.format(symbol=symbol))

    results = qry.result()
    results = [dict(row.items()) for row in results]
    resp = custom_jsonify(results)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    resp.headers.add('Access-Control-Allow-Methods', 'GET')
    return resp



