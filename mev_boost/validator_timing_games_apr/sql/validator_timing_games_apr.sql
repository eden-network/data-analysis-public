-- with skillz_pubkeys as (
--   select pubkey
--   from `eden-data-private.ethereum_auxiliary.mevboost_pics_pubkey_mapping`
--   where lower(lido_node_operator) like "%skillz%"
-- )
with bids as (
  select  p.block_timestamp,
          p.block_number,
          p.block_hash,
          p.slot,
          p.value as payload_value,
          p.proposer_fee_recipient,
          p.proposer_pubkey,
          p.relay,
          b.builder_pubkey,          
          b.value as bid_value,
          b.timestamp as bid_timestamp,
          p.reorged
  from `eden-data-private.mev_boost.payloads` p 
  join `eden-data-private.mev_boost.bids` b on b.block_hash = p.block_hash and b.block_timestamp = p.block_timestamp and b.relay = p.relay
  where p.block_timestamp >= timestamp("2023-11-01 00:00:00")
        and p.block_timestamp < timestamp("2023-12-01 00:00:00") --timestamp_sub(current_timestamp(), interval 28 day)         
), deduped_bids as (
  select  block_timestamp,
          block_number,
          block_hash,
          slot,
          payload_value,
          proposer_fee_recipient,
          proposer_pubkey,
          builder_pubkey,          
          bid_value,
          reorged,
          array_agg(distinct relay) as relay_claims,
          min(bid_timestamp) as bid_timestamp --take optimistic bid timestamp (earliest seen across relays)
  from bids
  group by  block_timestamp,
            block_number,
            block_hash,
            slot,
            payload_value,
            proposer_fee_recipient,
            proposer_pubkey,
            builder_pubkey,            
            bid_value,
            reorged
), winning_bids as (
  select  block_timestamp,
          block_number,
          block_hash,
          slot,
          payload_value,
          proposer_fee_recipient,
          proposer_pubkey,
          builder_pubkey,
          bid_value,
          relay_claims,          
          bid_timestamp,          
          (select max(b.`value`) from `eden-data-private.mev_boost.bids` b where db.block_timestamp = b.block_timestamp and b.`timestamp` <= timestamp_add(db.block_timestamp, interval 0 millisecond)) as bid_value_t0,
          row_number() over(partition by slot, block_hash, proposer_pubkey order by bid_timestamp) as builder_tie_break,
  from deduped_bids db  
  where reorged = false
), validator_timing_stats as (
    select  block_timestamp,  
            block_number,
            block_hash,
            slot,
            relay_claims,          
            payload_value,
            payload_value / 1e9 as payload_value_gwei,
            payload_value / 1e18 as payload_value_eth,
            proposer_fee_recipient,
            proposer_pubkey,          
            builder_pubkey,
            bid_timestamp,
            timestamp_diff(bid_timestamp, block_timestamp, millisecond) as ms_gt_t0,
            bid_value_t0,          
            bid_value_t0 / 1e9 as bid_value_t0_gwei,
            bid_value_t0 / 1e18 as bid_value_t0_eth,
            payload_value - bid_value_t0 as bid_delta,
            (payload_value - bid_value_t0) / 1e9 as bid_delta_gwei,
            (payload_value - bid_value_t0) / 1e18 as bid_delta_eth,
            builder_tie_break
    from winning_bids p 
    where builder_tie_break = 1 -- Note: this removes relay claims and builders that recieved/submitted the same block_hash that won but at later time than `first seen` across all relays
), timing_stats as (
  select    avg(ms_gt_t0) as avg_ms_diff,                      
            sum(bid_delta_eth) as total_bid_delta_eth,
            sum(payload_value_eth) as total_payload_value_eth,
            count(*) as `count`,
            -- proposer_pubkey,            
            concat(lower(coalesce(mppm.label, "unknown")), " - ", lower(coalesce(mppm.lido_node_operator, "unknown"))) as validator_operator
  from validator_timing_stats vts
  left join `eden-data-private.ethereum_auxiliary.mevboost_pics_pubkey_mapping` mppm on mppm.pubkey = vts.proposer_pubkey
  group by validator_operator
), reorg_stats as (
  select  concat(lower(coalesce(mppm.label, "unknown")), " - ", lower(coalesce(mppm.lido_node_operator, "unknown"))) as validator_operator,
          sum(case when not reorged then 1 else 0 end) as `count`,
          sum(case when reorged then 1 else 0 end) as reorged_count,
          sum(case when reorged then b.payload_value else 0 end) / 1e18 as lost_revenue,
          sum(case when not reorged then b.payload_value else 0 end) / 1e18 as revenue,
          sum(case when reorged then b.payload_value else 0 end) / sum(case when not reorged then b.payload_value else 0 end) * 100 as ratio_lost_via_reorg
  from deduped_bids b
  left join `eden-data-public.ethereum_auxiliary.mevboost_pics_pubkey_mapping` mppm on mppm.pubkey = b.proposer_pubkey  
  group by validator_operator
)
select  ts.validator_operator,
        -- ts.`count` as timing_count,
        ts.count as num_blocks,
        -- rs.`count` as reorg_count,
        rs.reorged_count as reorged, --actually_reorged,
        ts.avg_ms_diff,
        ts.total_bid_delta_eth as gained_via_timing,
        rs.lost_revenue as lost_via_reorg,
        ts.total_payload_value_eth,
        -- rs.revenue,
        -- ratio_lost_via_reorg,
        (rs.lost_revenue / ts.total_payload_value_eth) * 100 as lost_perc_via_reorg,
        (ts.total_bid_delta_eth / ts.total_payload_value_eth) * 100 as gained_perc_via_timiing
from timing_stats ts
left join reorg_stats rs on rs.validator_operator = ts.validator_operator
where ts.`count` > 1000
order by ts.avg_ms_diff desc