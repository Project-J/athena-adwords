{% macro stitch_adwords_criteria_performance() %}

    {{ adapter.dispatch('stitch_adwords_criteria_performance', 'adwords')() }}

{% endmacro %}


{% macro athena__stitch_adwords_criteria_performance() %}

with criteria_base as (

    select * from {{ var('criteria_performance_report') }}

), 

aggregated as (

    select
        
        cast(
            {{ dbt_utils.surrogate_key (
            [
             'customerid',
             'keywordid',
             'adgroupid',
             'day'
            ]
            ) }} as varchar) as id,
        
        cast(from_iso8601_timestamp(day) as date) as date_day,
        keywordid as criteria_id,
        adgroup as ad_group_name,
        adgroupid as ad_group_id,
        adgroupstate as ad_group_state,
        campaign as campaign_name,
        campaignid as campaign_id,
        campaignstate as campaign_state,
        customerid as customer_id,
        _sdc_report_datetime,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(cast((cast(cost as double)/cast(1000000 as double)) as decimal(38,6))) as spend

    from criteria_base
    {{ dbt_utils.group_by(11) }}

), 

ranked as (

    select
    
        *,
        rank() over (partition by id
            order by _sdc_report_datetime desc) as latest
            
    from aggregated

),

final as (

    select *
    from ranked
    where latest = 1

)

select * from final

{% endmacro %}