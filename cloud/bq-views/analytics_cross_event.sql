select
  date,
  symbol,
  (case
    when ao_event = 1 and ao_event_prior_period = 0 then 'death_cross'
    when ao_event = 0 and ao_event_prior_period = 1 then 'golden_cross'
  end) event
from (
SELECT 
  ao.date,
  ao.symbol,
  if(ao.sma_50 < ao.sma_200, 1, 0) ao_event,
  lag(if(ao.sma_50 < ao.sma_200, 1, 0)) over(order by symbol, date) ao_event_prior_period
FROM `ticker-224822.ticker_test_120718.analytics_output` ao
order by 
  ao.date desc
)
where
  ao_event != ao_event_prior_period