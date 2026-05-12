-- Carrega dados de Atracacao e Carga, resulta em um evento por Carga:
with atracacao as (
        select * from {{ ref('stg_atracacao') }}
    ),
    horas_espera as (
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
            case 
                when a.ds_tipo_operacao in ('Movimentação da Carga', 'Misto') then 'Comercial'
                when a.ds_tipo_operacao in ('Passageiro') then 'Passageiros'
                when a.ds_tipo_operacao in ('Reparo/Manutenção') then 'Manutenção'
                else 'Apoio/Outros'
            end as categoria_operacao
        from atracacao a
    ),
    comercial as (
        select *
        from horas_espera
        where categoria_operacao = 'Comercial'
    ),
    contagem_anos as (
        select 
            nm_porto,
            count(distinct extract(year from dh_chegada)) as total_anos_porto
        from comercial
        group by 1
    ),
    significancia as (
        select 
            c.*,
            a.total_anos_porto
        from comercial c
        inner join contagem_anos a on c.nm_porto = a.nm_porto
        where a.total_anos_porto >= 10
    )
    select * from significancia