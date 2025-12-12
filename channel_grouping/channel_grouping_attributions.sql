with processed_sources as (
    select
        source_category,
        -- Cleaning logic for source values
        replace(
            replace(
                lower(
                    case
                        when source not like '%.%' then source || '.'
                        else source
                    end
                ), ' ', ''
            ), '.', '\\.'
        ) as processed_source
    -- this is a manual load that will get automated in the future
    from source_category
    where source != 'google+'
    order by len(source) desc
),

lists as (
    select
        -- Using aggregation to create pipe-separated lists for each source category
        listagg(distinct case when source_category = 'SOURCE_CATEGORY_SEARCH' then processed_source end, '|') as seac,
        listagg(distinct case when source_category = 'SOURCE_CATEGORY_SHOPPING' then processed_source end, '|') as shoc,
        listagg(distinct case when source_category = 'SOURCE_CATEGORY_SOCIAL' then processed_source end, '|') as socc,
        listagg(distinct case when source_category = 'SOURCE_CATEGORY_VIDEO' then processed_source end, '|') as vidc
    from processed_sources
),

-- this is to make sure we do not split a session
all_ids as (
    select
        session_id,
        min(timestamp) as min_timestamp
    from all_events
    where ip_address != '0.0.0.0'
    group by 1
    having min(timestamp) >=
        {% if is_incremental() %}
            dateadd(day, -41, current_timestamp)
        {% else %}
            '2024-06-10'
        {% endif %}
        and min(timestamp) < current_date
),

array_agg_per_session as (
    select
        timestamp,
        min(timestamp) over (partition by session_id) as session_first_event_at,
        max(timestamp) over (partition by session_id) as session_last_event_at,
        array_sort(array_unique_agg(ip_address) over (partition by session_id)) as session_ips,
        array_sort(array_unique_agg(activity) over (partition by session_id)) as activities,
        page_referrer,
        session_id,
        anonymous_id,
        context_campaign_source,
        context_campaign_medium,
        context_campaign_name,
        context_campaign_content,
        context_campaign_term,
        supplier_services_db.work.fn_urldecode(page_url) as page_url,
        reguser_id,
        array_sort(array_unique_agg(iff(heading_id = 0, null, heading_id)) over (partition by session_id)) as heading_ids,
        test_is_bot,
        prior_event_ts
    from all_events
    where session_id in (select session_id from all_ids)
    -- help with optimization
        and timestamp >= (select min(min_timestamp) from all_ids)
),

first_campaign_params as (
    select
        timestamp,
        session_first_event_at,
        session_last_event_at,
        session_ips,
        activities,
        page_referrer,
        session_id,
        anonymous_id,
        page_url,
        reguser_id,
        heading_ids,
        test_is_bot,
        prior_event_ts,
        -- get the first set of campaign parameters that is not null
        first_value(context_campaign_source) ignore nulls over (partition by session_id order by timestamp asc) as context_campaign_source,
        first_value(context_campaign_medium) ignore nulls over (partition by session_id order by timestamp asc) as context_campaign_medium,
        first_value(context_campaign_name) ignore nulls over (partition by session_id order by timestamp asc) as context_campaign_name,
        first_value(context_campaign_content) ignore nulls over (partition by session_id order by timestamp asc) as context_campaign_content,
        first_value(context_campaign_term) ignore nulls over (partition by session_id order by timestamp asc) as context_campaign_term
    from array_agg_per_session
    -- Filter to get only the non-internal in the beginning of the sessions
    where abs(timestampdiff(minute, session_first_event_at, timestamp)) < 3
),

qualification as (
    select
        timestamp,
        session_first_event_at,
        session_last_event_at,
        session_ips,
        activities,
        page_referrer,
        session_id,
        anonymous_id,
        context_campaign_source,
        context_campaign_medium,
        context_campaign_name,
        context_campaign_content,
        context_campaign_term,
        page_url,
        reguser_id,
        heading_ids,
        test_is_bot,
        regexp_like(
            split_part(coalesce(page_referrer, 'direct'), '?', 1)
            -- start with http(s):// and 'thomas(-)net' is in the domain, appearing before any '/' character
            , '^https?://[^/]*(thomas-?net)[^/]*/.*$'
        ) as is_internal
    from first_campaign_params
    -- Filtering first row per session according to timestamp
    -- And non-internal pagereferrer (The idea is to make sure we're bringing the correct first event, it must be null (Direct) or external URL)
    qualify row_number() over (
        partition by session_id
        -- first is either external or direct, then the earliest between them
        order by 
            is_internal asc,
            -- for ties
            timestamp asc,
            prior_event_ts asc
    ) = 1
),

-- The last-non-internal rule is applied here
-- When the page_referrer is a thomasnet domain
-- The lag function is used to return the non-thomasnet-domain preceding row across the anonymous_id partition
last_non_internal as (
    select
        is_internal,
        -- TODO: missing this same logic for utm parameters
        case
            when is_internal = true
                then last_value(
                    case
                        -- avoid direct being treated the same as internal
                        when page_referrer is null and is_internal = false then '(direct)'
                        when is_internal = false then page_referrer
                    end
                ) ignore nulls over(
                    partition by anonymous_id
                    order by timestamp asc
                    range between interval '30 days' preceding and current row
                )
            else page_referrer
        end as last_non_internal,
        timestamp,
        session_first_event_at,
        session_last_event_at,
        session_ips,
        activities,
        session_id,
        anonymous_id,
        context_campaign_source,
        context_campaign_medium,
        context_campaign_name,
        context_campaign_content,
        context_campaign_term,
        page_url,
        reguser_id,
        heading_ids,
        test_is_bot
    from qualification
),

-- Including different parameter aliases for utm_source, utm_medium and utm_campaign
-- Extracting click IDs from URLs
-- Creating utm_flag
utms_parsing as (
    select
        -- TODO: missing additional logic for organic
        split_part(regexp_substr(page_url, '(campaign_type|utm_source)=([^&]*)'), '=', 2) as source_parsed,
        split_part(regexp_substr(page_url, '(channel|utm_medium)=([^&]*)'), '=', 2) as medium_parsed,
        split_part(regexp_substr(page_url, '(campaign_name|utm_campaign)=([^&]*)'), '=', 2) as campaign_parsed,
        coalesce(
            context_campaign_source,
            case
                when source_parsed is null or source_parsed = '' then '(direct)'
                else source_parsed
            end
        ) as utm_source,
        coalesce(
            context_campaign_medium,
            case
                when medium_parsed is null or medium_parsed = '' then '(not set)'
                else medium_parsed
            end
        ) as utm_medium,
        coalesce(
            context_campaign_name,
            case
                when campaign_parsed is null or campaign_parsed = '' then '(not set)'
                else campaign_parsed
            end
        ) as utm_campaign,
        coalesce(
            (utm_source is not null and utm_source != '(direct)')
            or (utm_medium is not null and utm_medium != '(not set)')
            or (utm_campaign is not null and utm_campaign != '(not set)'),
            false
        ) as utm_flag,
        split_part(regexp_substr(page_url, '(gclid|msclkid|msockid|fbclid|li_fat_id|dclid|wbraid|gbraid|li_giant)=([^&]*)'), '=', 1) as clickdesc,
        split_part(regexp_substr(page_url, '(gclid|msclkid|msockid|fbclid|li_fat_id|dclid|wbraid|gbraid|li_giant)=([^&]*)'), '=', 2) as clickid,
        case
            when clickdesc in ('li_giant', 'li_fat_id') and last_non_internal is null then 'https://www.linkedin.com/'
            when last_non_internal = '(direct)' then null
            else last_non_internal
        end as last_non_internal,
        timestamp,
        session_first_event_at,
        session_last_event_at,
        session_ips,
        activities,
        session_id,
        anonymous_id,
        context_campaign_source,
        context_campaign_medium,
        context_campaign_name,
        page_url,
        context_campaign_content,
        context_campaign_term,
        reguser_id,
        heading_ids,
        test_is_bot
    from last_non_internal
),

final as (
    select
        utm.session_id,
        utm.reguser_id,
        utm.anonymous_id,
        utm.timestamp,
        utm.session_first_event_at,
        utm.session_last_event_at,
        utm.heading_ids,
        utm.session_ips,
        utm.activities,
        utm.utm_source,
        utm.utm_medium,
        utm.utm_campaign,
        utm.context_campaign_content as utm_content,
        utm.context_campaign_term as utm_term,
        case
            when (utm.last_non_internal is null or utm.last_non_internal in ('', '/')) and regexp_like(split_part(utm.page_url, '?', 1), '.*(thomas-?net).*') and utm.utm_flag = false and utm.clickdesc is null then 'Direct'
            when utm.clickdesc in ('wbraid', 'gbraid', 'gclid') and not regexp_like(utm.last_non_internal, '.*(\\/|\\.)(' || lists.seac || '|' || lists.shoc || '|' || lists.socc || '|' || lists.vidc || '|syndicatedsearch).*') then 'Display'
            when regexp_like(utm.utm_medium, '^(.*cp.*|ppc|retargeting|paid.*)$') and regexp_like(utm.last_non_internal, '.*(\\/|\\.)(' || lists.vidc || ').*') then 'Paid Video'
            when regexp_like(utm.utm_medium, '^(.*cp.*|ppc|retargeting|paid.*)$') and regexp_like(utm.last_non_internal, '.*(\\/|\\.)(' || lists.shoc || ').*') then 'Paid Shopping'
            when regexp_like(utm.utm_medium, '^(.*cp.*|ppc|retargeting|social|website|paid.*)$') and (rlike(utm.utm_source, '(facebook|linkedin|reddit).*') or regexp_like(utm.last_non_internal, '.*(\\/|\\.)(' || lists.socc || ').*')) then 'Paid Social'
            when regexp_like(utm.utm_medium, '^(.*cp.*|ppc|retargeting|website|paid.*)$') and (rlike(utm.utm_source, '(bing|adwords)') or regexp_like(utm.last_non_internal, '.*(\\/|\\.)(' || lists.seac || '|syndicatedsearch).*')) then 'Paid Search'
            when lower(utm.utm_medium) in ('email', 'e-mail', 'e_mail', 'e mail') or lower(utm.utm_source) in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
            when regexp_like(utm.last_non_internal, '.*(chatgpt\\.com).*') or lower(utm.utm_source) in ('chatgpt.com') then 'Organic Search'
            when regexp_like(utm.last_non_internal, '(http|android).*(\\/|\\.)(' || lists.vidc || ').*') then 'Video'
            when regexp_like(utm.last_non_internal, '(http|android).*(\\/|\\.)(' || lists.shoc || ').*') then 'Shopping'
            when regexp_like(utm.last_non_internal, '(http|android).*(\\/|\\.)(' || lists.socc || ').*') then 'Social'
            when regexp_like(utm.last_non_internal, '(http|android).*(\\/|\\.)(' || lists.seac || '|brave).*') then 'Organic Search'
            when utm.last_non_internal is not null and utm.utm_flag = false and utm.clickdesc is null then 'Referral'
            else 'Unassigned'
        end as channel_group,
        'last_non_internal_origin' as attribution_model,
        utm.page_url as landing_page_url,
        utm.last_non_internal as raw_referrer_url,
        utm.clickdesc as click_desc,
        utm.clickid as click_id,
        utm.utm_flag as has_utm_params,
        coalesce(channel_group in ('Organic Search'), false) as is_organic_channel,
        coalesce(channel_group in ('Paid Search', 'Paid Social', 'Paid Video', 'Paid Shopping', 'Display'), false) as is_paid_channel
    from utms_parsing as utm
    -- one row table
    cross join lists
)

{% if is_incremental() %}
, diff as (
    select * from final
    except
    select * exclude (created_at, updated_at)
    from {{ this }}
)
{% endif %}

select
    -- primary key
    session_id,

    -- foreign keys
    reguser_id,
    anonymous_id,

    -- dates and timestamps
    timestamp,
    session_first_event_at,
    session_last_event_at,
    current_timestamp as created_at,
    current_timestamp as updated_at,

    -- attributes
    heading_ids,
    session_ips,
    activities,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    channel_group,
    attribution_model,
    landing_page_url,
    raw_referrer_url,
    click_desc,
    click_id,

    -- booleans
    has_utm_params,
    is_organic_channel,
    is_paid_channel
{% if is_incremental() %}
    from diff
    where session_first_event_at >= current_date - 10
{% else %}
    from final
{% endif %}
