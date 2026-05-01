
with base as (
    select * from {{ ref('int_atracacao_carga') }}
),
group_by_operacao as (
select
    nm_porto,
    nm_complexo_portuario,
    categoria_operacao,
    cast(split_part(max(ds_coordenadas), ',', 1) as numeric) as nr_longitude,
    cast(split_part(max(ds_coordenadas), ',', 2) as numeric) as nr_latitude,
    count(distinct id_atracacao) as total_atracacoes,
    round(avg(atracacao_horas_espera)::numeric, 2) as media_horas_espera,
    round(sum(total_peso_atracacao)::numeric, 2) as peso_total_movimentado
from base
group by 1, 2, 3
order by media_horas_espera desc
),
significancia as (
    select *
    from group_by_operacao
    where categoria_operacao in ('Comercial') 
    and total_atracacoes >= 10
)
select * from significancia
