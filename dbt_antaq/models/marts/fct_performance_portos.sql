with base_deduplicada as (
    -- Etapa 1: Retirar duplicados devido ao join com carga, mantendo o registro mais recente por ID de atracação e natureza de carga
    select
        id_atracacao,
        nm_porto,
        nm_complexo_portuario,
        categoria_operacao,
        ds_natureza_carga, -- Visão por natureza
        max(ds_coordenadas) as ds_coordenadas,
        max(atracacao_horas_espera) as horas_espera_unica
    from {{ ref('int_atracacao_carga') }}
    where categoria_operacao = 'Comercial' -- Filtro de comercial
    group by 1, 2, 3, 4, 5
),

metricas_finais as (
    -- Etapa 2: Agora a média e o count(distinct) são seguros e rápidos
    select
        nm_porto,
        nm_complexo_portuario,
        ds_natureza_carga,
        max(ds_coordenadas) as coordenadas_raw,
        count(id_atracacao) as total_atracacoes_natureza, 
        round(avg(horas_espera_unica)::numeric, 2) as media_horas_espera
    from base_deduplicada
    group by 1, 2, 3
)

select 
    nm_porto,
    nm_complexo_portuario,
    ds_natureza_carga,
    cast(split_part(coordenadas_raw, ',', 1) as numeric) as nr_longitude,
    cast(split_part(coordenadas_raw, ',', 2) as numeric) as nr_latitude,
    total_atracacoes,
    media_horas_espera
from metricas_finais
where total_atracacoes >= 10
order by nm_porto, ds_natureza_carga, media_horas_espera desc