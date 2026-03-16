with 

source as (

    select * from {{ source('Raw_data', 'euro2024_players_images_matched') }}

),

renamed as (

    select
        player_id,
        player_name,
        team,
        market_value,
        current_club_name,
        date_of_birth,
        age,
        matched_player_id,
        matched_player_name,
        image_url,
        player_profile_url,
        match_method

    from source

)

select * from renamed