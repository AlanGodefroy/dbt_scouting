with 

source as (

    select * from {{ source('Raw_data', 'UEFA_EURO_lineup') }}

),

renamed as (

    select
        CAST(player_id AS INT64) AS player_id,
        CAST(TRUNC(player_id) AS INT64) AS player_id,
        player_name,
        player_nickname,
        jersey_number,
        country,
        cards,
        positions,
        match_id,
        team,
        position_name

    from source

)

select * from renamed