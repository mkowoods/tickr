SELECT 
  events.date,
  events.symbol,
  events.event,
  stats.adj_close,
  (stats.adj_close / stats.adj_close_lag_1) return_lag_1,
  (stats.adj_close / stats.adj_close_lag_5) return_lag_5,
  (stats.adj_close / stats.adj_close_lag_10) return_lag_10,
  (stats.adj_close / stats.adj_close_lag_20) return_lag_20,
  
  (stats.adj_close_lead_1 / stats.adj_close) return_lead_1,
  (stats.adj_close_lead_5 / stats.adj_close) return_lead_5,
  (stats.adj_close_lead_10 / stats.adj_close) return_lead_10,
  (stats.adj_close_lead_20 / stats.adj_close) return_lead_20,
  
  (stats.min_lead_20 / stats.adj_close) return_min_lead_20,
  (stats.max_lead_20 / stats.adj_close) return_max_lead_20,
  
  min_lead_20,
  max_lead_20,
  std_lead_20,
  std_lead_20/mean_lead_20 norm_vol_lead_20,
  
  min_lead_50,
  max_lead_50,
  std_lead_50,
  std_lead_50/mean_lead_50 norm_vol_lead_50
  
FROM `ticker-224822.ticker_test_120718.events` as events
join `ticker-224822.ticker_test_120718.analytics_view` as stats
on events.date = stats.date
and events.symbol = stats.symbol
