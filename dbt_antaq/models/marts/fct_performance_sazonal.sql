-- Fazer um inner join com a view de int pra trazer  o indice de sazonalidade pra comparar com o boxplot de cada mes:
--inner join pelo nm_porto, nr_mes...
{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_atracacao_carga') }}
),

medias_mensais as (
    select
        
        -- 1. Mantemos a data truncada para o primeiro dia do mês (Série Temporal)
        cast(date_trunc('month', dh_chegada) as date) as mes_referencia,
        
        -- 2. Extraímos o mês e ano como dimensões auxiliares
        extract(month from dh_chegada) as nr_mes,
        extract(year from dh_chegada) as nr_ano,
        
        nm_porto,
        
        -- 3. Cálculo da métrica
        round(avg(atracacao_horas_espera)::numeric, 2) as media_mensal_horas_espera,
        count(distinct id_atracacao) as total_atracacoes,
    from base
    left join indice_sazonal on indice_sazonal.nr_mes = extract(month from base.dh_chegada)
    group by 1, 2, 3, 4
    order by mes_referencia asc, nm_porto asc
),

significancia as (
    select *
    from medias_mensais
    where total_atracacoes >= 10
)

select * from significancia
