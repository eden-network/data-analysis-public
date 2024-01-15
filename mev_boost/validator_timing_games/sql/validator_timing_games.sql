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
          b.timestamp as bid_timestamp         
  from `eden-data-public.mev_boost.payloads` p 
  join `eden-data-public.mev_boost.bids` b on b.block_hash = p.block_hash and b.block_timestamp = p.block_timestamp and b.relay = p.relay
  where p.block_timestamp > timestamp_sub(current_timestamp(), interval 28 day) and p.reorged = false
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
            bid_value
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
          (select max(b.`value`) from `eden-data-public.mev_boost.bids` b where db.block_timestamp = b.block_timestamp and b.`timestamp` <= timestamp_add(db.block_timestamp, interval 0 millisecond)) as bid_value_t0,
          row_number() over(partition by slot, block_hash, proposer_pubkey order by bid_timestamp) as builder_tie_break,
  from deduped_bids db  
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
)
select    ms_gt_t0 as ms_diff,          
          bid_delta_gwei,
          bid_delta_eth,
          block_number,
          proposer_pubkey,
          builder_pubkey,
          concat(lower(coalesce(mppm.label, "unknown")), " - ", lower(coalesce(mppm.lido_node_operator, "unknown"))) as validator_operator
from validator_timing_stats vts
left join `eden-data-public.ethereum_auxiliary.mevboost_pics_pubkey_mapping` mppm on mppm.pubkey = vts.proposer_pubkey;