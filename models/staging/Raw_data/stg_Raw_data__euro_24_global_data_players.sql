with 

source as (

    select * from {{ source('Raw_data', 'euro_24_global_data_players') }}

),

renamed as (

    select
        CAST(TRUNC(player_id) AS INT64) AS player_id,
        player_name,
        team,
        market_value,
        current_club_name,
        date_of_birth

    from source

)

select * from renamed