{#
  Job A auto-chaff scoring = tmp_digital_occ_auto_chaff. FAITHFUL 1:1 transliteration of:
    get_all_ad_sizes        -> vw_standard_ad_size (inlined CTE; legacy makes a temp view)
    get_all_known_ad_servers-> vw_known_ad_server  (inlined CTE; legacy makes a temp view)
    get_auto_chaff_records  -> cte_bis_data / cte_scores / cte_derive_auto_chaff / cte_dedup_autochaf
  Output columns = legacy tmp_digital_occ_auto_chaff (auto_chaff_df.drop("rnum")): the 4 carried columns
  + 7 scores + is_auto_chaff. Consumed by crtv_staging_excluded (exclude anti-join on
  creative_url_hash + provider_code) and crtv_autochaff_records (join back to the candidate).

  No-op for CTV (cte_bis_data filters provider_code='BIS'; CTV is 'AVOD BISCTV') — but the full logic
  is present. Spark->Trino swaps only: temp views -> CTEs, Nvl->coalesce, Regexp_*->regexp_*,
  SUBSTRING_INDEX-> macro, POSITION(x, y)->strpos(y, x).
#}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'job_a'],
    views_enabled=false
) }}

{%- set country_code_US    = "'US'" -%}
{%- set status_flag_active = "'ACTIVE'" -%}
{%- set source_bis_code    = "'BIS'" -%}
{%- set occ_media_type_video  = "'video'" -%}
{%- set occ_media_type_html5  = "'html5'" -%}
{%- set occ_media_type_flash  = "'flash'" -%}
{%- set occ_media_type_banner = "'banner'" -%}
{%- set occ_media_type_banner_upper = "'BANNER'" -%}

{#- click_through_score host: SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(domain_url,'/',3),'://',-1),'/',1),'?',1) -#}
{%- set h1 -%}{{ substring_index('tdr.domain_url', '/', 3) }}{%- endset -%}
{%- set h2 -%}{{ substring_index(h1, '://', -1) }}{%- endset -%}
{%- set h3 -%}{{ substring_index(h2, '/', 1) }}{%- endset -%}
{%- set domain_host -%}{{ substring_index(h3, '?', 1) }}{%- endset -%}

with

-- get_all_ad_sizes -> vw_standard_ad_size
vw_standard_ad_size as (
    with cte_ad_size as (
        select distinct size_id,
            size_name,
            size_width,
            size_height,
            country_iso_2_code,
            inserted_at_timestamp as record_created_timestamp
        from {{ source('km_preparation_db', 'standard_ad_size') }}
        where country_iso_2_code = {{ country_code_US }} and record_status_flag = {{ status_flag_active }}
        and size_id != 5
    ),
    cte_adsize_dedup as (
        select row_number() over (partition by size_width, size_height, country_iso_2_code order by size_name desc) as rnum, *
        from cte_ad_size
    )
    select
        size_id,
        size_name,
        size_width,
        size_height,
        country_iso_2_code,
        record_created_timestamp
    from cte_adsize_dedup where rnum = 1
),

{#- get_all_known_ad_servers -> vw_known_ad_server -#}
vw_known_ad_server as (
    select
        clean_ad_server as ad_server
    from (
        select
            row_number() over (partition by clean_ad_server order by inserted_at_timestamp desc) rnum,
            clean_ad_server
        from (
            select
                coalesce(regexp_extract(lower(coalesce(regexp_replace(ad_server, '[\n\r]+', ''), ' ')),
                                        '^(?:https?://)?(?:[@\n]+@)?(?:www.)?([^:/\n?]+)', 1), ''
                ) as clean_ad_server,
                inserted_at_timestamp
            from {{ source('km_preparation_db', 'adscore_provided_adservers') }}
            where record_status_flag = {{ status_flag_active }}
        ) ad_score_adserver
    )
    where rnum = 1
),

cte_bis_data as (
    select * from {{ ref('crtv_staging_candidate') }} where provider_code = {{ source_bis_code }}
),

cte_scores as (
    select creative_url_hash,
    provider_code,
    creative_type,
    creative_duration,
    0 as data_provider_code_score,
    case
    when coalesce(cast(sas.size_id as varchar), '') != '' then
        30
        else
        0
    end as ad_size_score,
    case when tdr.creative_type = {{ occ_media_type_html5 }} then
            1500
        when tdr.creative_type = {{ occ_media_type_flash }} then
            330
        else
            0
        end as creative_type_score,
        case when coalesce(ads.ad_server, '') != '' then
                500
            else
                0
            end as ad_server_score,

        case when (tdr.occurrence_description like '%/ad/%' or
                                tdr.occurrence_description like '%/ads/%' or
                                tdr.occurrence_description like '%/campaign/%' or
                                tdr.occurrence_description like '%/campaigns/%' or
                                tdr.occurrence_description like '%/promotional/%' or
                                tdr.occurrence_description like '%/sponsor/%' ) then
                    150
                when (tdr.occurrence_description like '%/content/%') then
                        -150
                    else
                        0
                    end as href_score,

        case when (tdr.occurrence_creative_url like '%/ad/%' or
                                tdr.occurrence_creative_url like '%/ads/%' or
                                tdr.occurrence_creative_url like '%/campaign/%' or
                                tdr.occurrence_creative_url like '%/campaigns/%' or
                                tdr.occurrence_creative_url like '%/promotional/%' or
                                tdr.occurrence_creative_url like '%/sponsor/%' ) then
                        150
                when (tdr.occurrence_creative_url like '%/content/%') then
                        -150
                else
                        0
                end as creative_url_score,

        case when coalesce(tdr.occurrence_description, '') != '' then
                    case when strpos(tdr.occurrence_description, {{ domain_host }}) > 0 then
                        160
                        else
                        300
                        end
                    else
                        0
                    end as click_through_score
        from cte_bis_data tdr
        left join vw_standard_ad_size sas
            on tdr.creative_width between sas.size_width - 10 and sas.size_width + 10
            and tdr.creative_height between sas.size_height - 10 and sas.size_height + 10
            and tdr.country_iso_2_code = sas.country_iso_2_code
            left join vw_known_ad_server ads
            on tdr.occurrence_creative_url_domain = ads.ad_server
),

cte_derive_auto_chaff as (
    select *,
        case when provider_code = {{ source_bis_code }} and creative_type = {{ occ_media_type_video }} then
            case when coalesce(creative_duration, 30) between 2 and 220 then
            false
            else
            true
            end
        when provider_code = {{ source_bis_code }} and creative_type in ({{ occ_media_type_html5 }}, {{ occ_media_type_flash }}, {{ occ_media_type_banner }}, {{ occ_media_type_banner_upper }}) then
            case when (ad_size_score + data_provider_code_score + click_through_score + creative_type_score + ad_server_score + href_score + creative_url_score  ) >= 315 then
            false
            else
            true
            end
        else
            false
        end as is_auto_chaff
    from cte_scores
),

cte_dedup_autochaf as (
    select *, row_number() over (partition by creative_url_hash order by creative_url_hash desc) as rnum
    from cte_derive_auto_chaff where is_auto_chaff = true
)

{#- final: SELECT * FROM cte_dedup_autochaf WHERE rnum = 1, then drop("rnum") -#}
select
    creative_url_hash,
    provider_code,
    creative_type,
    creative_duration,
    data_provider_code_score,
    ad_size_score,
    creative_type_score,
    ad_server_score,
    href_score,
    creative_url_score,
    click_through_score,
    is_auto_chaff
from cte_dedup_autochaf
where rnum = 1
