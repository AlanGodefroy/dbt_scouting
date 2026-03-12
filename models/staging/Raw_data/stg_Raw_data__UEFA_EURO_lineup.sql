with 

source as (

    select * from {{ source('Raw_data', 'UEFA_EURO_lineup') }}

),

renamed as (

    select
<<<<<<< HEAD
        CAST(player_id AS INT64) AS player_id,
=======
        CAST(TRUNC(player_id) AS INT64) AS player_id,
>>>>>>> 7efaf2181c5683624416ab7163cebe8660870c0d
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