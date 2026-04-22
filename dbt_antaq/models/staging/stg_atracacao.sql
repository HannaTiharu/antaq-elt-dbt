with source as (
    select * from {{ source('antaq_raw', 'raw_atracacao') }}
),

renamed as (
    select
        cast("IDAtracacao" as int) as id_atracacao,
        "Porto Atracação" as nm_porto,
        "Coordenadas" as ds_coordenadas,
        "Complexo Portuário" as nm_complexo_portuario,
        to_timestamp("Data Chegada", 'DD/MM/YYYY HH24:MI:SS') as dh_chegada,
        to_timestamp("Data Atracação", 'DD/MM/YYYY HH24:MI:SS') as dh_atracacao,
        to_timestamp("Data Início Operação", 'DD/MM/YYYY HH24:MI:SS') as dh_inicio_operacao,
        to_timestamp("Data Término Operação", 'DD/MM/YYYY HH24:MI:SS') as dh_termino_operacao,
        to_timestamp("Data Desatracação", 'DD/MM/YYYY HH24:MI:SS') as dh_desatracacao, 
        "FlagMCOperacaoAtracacao" as ds_motivo_atracacao
    from source
)

select * from renamed