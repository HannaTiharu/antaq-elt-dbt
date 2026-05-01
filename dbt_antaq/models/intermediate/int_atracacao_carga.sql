-- Carrega dados de Atracacao e Carga, resulta em um evento por Carga:
with atracacao as (
        select * from {{ ref('stg_atracacao') }}
    ),
    carga as (
        select * from {{ ref('stg_carga') }}
    ), 
    joined as (
        select
            a.id_atracacao,
            a.nm_porto,
            a.ds_coordenadas,
            a.nm_complexo_portuario,
            a.dh_chegada,
            a.dh_atracacao,
            a.dh_inicio_operacao,
            a.dh_termino_operacao,
            a.dh_desatracacao,
            a.ds_motivo_atracacao,
            extract(epoch from (a.dh_atracacao - a.dh_chegada)) / 3600 as atracacao_horas_espera,
            c.id_carga,
            c.cd_mercadoria,
            c.ds_natureza_carga,
            c.vl_peso_carga,
            case 
                when a.ds_tipo_operacao in ('Movimentação da Carga', 'Misto') then 'Comercial'
                when a.ds_tipo_operacao in ('Passageiro') then 'Passageiros'
                when a.ds_tipo_operacao in ('Reparo/Manutenção') then 'Manutenção'
                else 'Apoio/Outros'
            end as categoria_operacao
        from atracacao a
        left join carga c on a.id_atracacao = c.id_atracacao
    )
select * from joined 