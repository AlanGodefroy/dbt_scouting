with 

source as (

    select * from {{ source('Raw_data', 'leverkusen_players_date_birth') }}

),

renamed as (

    select
        player_name,
        date_of_birth

    from source

)

select * from renamed