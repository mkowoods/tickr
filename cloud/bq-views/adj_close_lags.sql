SELECT 
  date,
  symbol,
  adj_close,
  lag(adj_close, 1) over(partition by symbol order by date asc) adj_close_lag_1,
  lag(adj_close, 5) over(partition by symbol order by date asc) adj_close_lag_5,
  lag(adj_close, 10) over(partition by symbol order by date asc) adj_close_lag_10,
  lag(adj_close, 20) over(partition by symbol order by date asc) adj_close_lag_20,
  lead(adj_close, 1) over(partition by symbol order by date asc) adj_close_lead_1,
  lead(adj_close, 5) over(partition by symbol order by date asc) adj_close_lead_5,
  lead(adj_close, 10) over(partition by symbol order by date asc) adj_close_lead_10,
  lead(adj_close, 20) over(partition by symbol order by date asc) adj_close_lead_20
FROM `ticker-224822.ticker_test_120718.stocks` 