{{ config(
    materialized='table',
    schema=var('target_dataset_id'),
    cluster_by=['Campaign', 'AdGroupName', 'Date']
) }}

select
  cast(gar.date as date) as Date,
  gar.year_month as YearMonth,
  concat(
    format_date('%b %d %Y', date_trunc(gar.date, week(monday))),
    ' - ',
    format_date('%b %d %Y', date_add(date_trunc(gar.date, week(monday)), interval 6 day))
  ) as YearWeek,
  extract(isoweek from gar.date) as Week,
  gar.account_name as AccountName,
  gar.currency as Currency,
  gar.campaign as Campaign,
  gar.campaign_id as CampaignID,
  gar.ad_group_name as AdGroupName,
  gar.ad_group_id as AdGroupID,
  initcap(trim(replace(gar.advertising_channel_type, '_', ' '))) as AdvertisingChannelType,
  gar.spend as Cost,
  gar.impressions as Impressions,
  gar.clicks as Clicks,
  gar.conversions as Conversions,
  gar.conversion_value as ConversionValue,
  gar.video_views as video_views,
  current_timestamp() as _etl_timestamp,
  concat('kulin-marketing-data-hub.', '{{ var("target_dataset_id") }}') as _source_dataset
from `kulin-marketing-data-hub.{{ var("target_dataset_id") }}.google_ads_campaign_performance` gar
