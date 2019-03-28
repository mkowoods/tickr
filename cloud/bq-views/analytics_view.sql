select
  date,
  adj_close,
  symbol,
  
  if(row_number <= 20, null, sma_20) sma_20,
  if(row_number <= 20, null, std_20) std_20,
  if(row_number <= 50, null, sma_50) sma_50,
  if(row_number <= 200, null, sma_200) sma_200,
  (adj_close - sma_20) / 2*std_20 bb_perc_20,
  
  lag(adj_close, 1) over(partition by symbol order by date asc) adj_close_lag_1,
  lag(adj_close, 5) over(partition by symbol order by date asc) adj_close_lag_5,
  lag(adj_close, 10) over(partition by symbol order by date asc) adj_close_lag_10,
  lag(adj_close, 20) over(partition by symbol order by date asc) adj_close_lag_20,
  lead(adj_close, 1) over(partition by symbol order by date asc) adj_close_lead_1,
  lead(adj_close, 5) over(partition by symbol order by date asc) adj_close_lead_5,
  lead(adj_close, 10) over(partition by symbol order by date asc) adj_close_lead_10,
  lead(adj_close, 20) over(partition by symbol order by date asc) adj_close_lead_20,

  min(adj_close) over(partition by symbol order by date asc ROWS 20 PRECEDING) min_lag_20,
  max(adj_close) over(partition by symbol order by date asc ROWS 20 PRECEDING) max_lag_20,
  AVG(adj_close) over(partition by symbol order by date asc ROWS 20 PRECEDING) mean_lag_20,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS 20 PRECEDING) std_lag_20,

  min(adj_close) over(partition by symbol order by date asc ROWS 50 PRECEDING) min_lag_50,
  max(adj_close) over(partition by symbol order by date asc ROWS 50 PRECEDING) max_lag_50,
  avg(adj_close) over(partition by symbol order by date asc ROWS 50 PRECEDING) mean_lag_50,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS 50 PRECEDING) std_lag_50,

  min(adj_close) over(partition by symbol order by date asc ROWS 200 PRECEDING) min_lag_200,
  max(adj_close) over(partition by symbol order by date asc ROWS 200 PRECEDING) max_lag_200,
  avg(adj_close) over(partition by symbol order by date asc ROWS 200 PRECEDING) mean_lag_200,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS 200 PRECEDING) std_lag_200,

  min(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) min_lead_20,
  max(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) max_lead_20,
  AVG(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) mean_lead_20,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) std_lead_20,

  min(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 50 FOLLOWING) min_lead_50,
  max(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 50 FOLLOWING) max_lead_50,
  avg(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 50 FOLLOWING) mean_lead_50,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 50 FOLLOWING) std_lead_50,

  min(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 200 FOLLOWING) min_lead_200,
  max(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 200 FOLLOWING) max_lead_200,
  avg(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 200 FOLLOWING) mean_lead_200,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS BETWEEN CURRENT ROW AND 200 FOLLOWING) std_lead_200

from (
SELECT 
  *,
  AVG(adj_close) over(partition by symbol order by date asc ROWS BETWEEN 20 PRECEDING and CURRENT ROW) sma_20,
  STDDEV(adj_close) over(partition by symbol order by date asc ROWS BETWEEN 20 PRECEDING and CURRENT ROW) std_20,
  AVG(adj_close) over(partition by symbol order by date asc ROWS BETWEEN 50 PRECEDING and CURRENT ROW) sma_50,
  AVG(adj_close) over(partition by symbol order by date asc ROWS BETWEEN 200 PRECEDING and CURRENT ROW) sma_200,
  

  ROW_NUMBER() over(partition by symbol order by date asc) row_number
FROM `ticker-224822.ticker_test_120718.stocks`
)