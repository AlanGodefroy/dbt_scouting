with 

source as (

    select * from {{ source('Raw_data', 'caracteristiques_joueurs_all') }}

),

renamed as (

    select
        player_id,
        first_name,
        last_name,
        `name` as player_name,
        last_season,
        current_club_id,
        player_code,
        country_of_birth,
        city_of_birth,
        country_of_citizenship,
        date_of_birth,
        sub_position,
        position,
        foot,
        height_in_cm,
        contract_expiration_date,
        agent_name,
        image_url,
        `url` as player_url,
        current_club_domestic_competition_id,
        current_club_name,
        market_value_in_eur,
        highest_market_value_in_eur

    from source

)

select * from renamed