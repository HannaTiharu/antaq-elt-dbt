{{ config(materialized='table') }}
-- Carrega o indice de sazonalidade da espera para eventos de comercio por porto e mês, comparando a média mensal com a média global para cada porto:
with base as (
        select * from {{ ref('int_atracacao_comercial') }}
    ),

    media_mensal as (
        select
            extract(month from dh_chegada) as nr_mes,
            nm_porto,
            round(avg(atracacao_horas_espera)::numeric, 2) as media_mensal_horas_espera
        from base
        group by 1, 2
        order by 2, 1 asc
    ),

    media_global as (
        select
            nm_porto,
            round(avg(atracacao_horas_espera)::numeric, 2) as media_global_horas_espera
        from base
        group by 1
    ),

    sazonalidade as (
        select
            m.nr_mes,
            m.nm_porto,
            m.media_mensal_horas_espera,
            g.media_global_horas_espera,
            case 
                when g.media_global_horas_espera = 0 then 0
                else round((m.media_mensal_horas_espera / g.media_global_horas_espera)::numeric, 2)
            end as indice_sazonalidade
        from media_mensal m
        left join media_global g on m.nm_porto = g.nm_porto
    )
select * from sazonalidade