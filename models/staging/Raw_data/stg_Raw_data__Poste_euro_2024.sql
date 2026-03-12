with 

source as (

    select * from {{ source('Raw_data', 'Poste_euro_2024') }}

),

renamed as (

    select
        CAST(TRUNC(player_id) AS INT64) AS player_id,
        player_name,
        attack,
        middle,
        defense,
        goal,
        poste

    from source

)

select * from renamed