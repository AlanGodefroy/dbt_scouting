with events as (
    select * from {{ ref('stg_Raw_data__Events_euro_2024') }}
),

poste as (
    select * from {{ ref('stg_Raw_data__Poste_euro_2024') }}
),

joined as (
    select
        events.*,
        poste.poste
    from events
    left join poste
        on events.player_id = poste.player_id
)

select * from joined