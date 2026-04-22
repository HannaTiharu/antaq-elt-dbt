with source as (
    select * from {{ source('antaq_raw', 'raw_carga') }}
),

renamed as (
    select
        cast("IDCarga" as int) as id_carga,
        cast("IDAtracacao" as int) as id_atracacao,
        "CDMercadoria" as cd_mercadoria,
        "Natureza da Carga" as ds_natureza_carga,
        cast(replace("VLPesoCargaBruta", ',', '.') as decimal(18,2)) as vl_peso_carga
    from source
)

select * from renamed