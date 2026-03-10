with 

source as (

    select * from {{ source('Raw_data', 'Poste_Leverkusen') }}

),

renamed as (

    select
        player_name,
        attack,
        middle,
        defense,
        goal,
        poste

    from source

)

select * from renamed