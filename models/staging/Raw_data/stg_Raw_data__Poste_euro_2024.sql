with 

source as (

    select * from {{ source('Raw_data', 'Poste_euro_2024') }}

),

renamed as (

    select
        cast(player_id as int64) as player_id,
        player_name,
        attack,
        middle,
        defense,
        goal,
        poste

    from source

)

select * from renamed