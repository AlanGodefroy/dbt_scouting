with 

source as (

    select * from {{ source('Raw_data', 'lineup_leverkusen') }}

),

renamed as (

    select
        player_id,
        player_name,
        player_nickname,
        jersey_number,
        country,
        cards,
        positions,
        match_id,
        position_name

    from source

)

select * from renamed