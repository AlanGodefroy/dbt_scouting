with 

source as (

    select * from {{ source('Raw_data', 'lineup_leverkusen') }}

),

renamed as (

    select

    from source

)

select * from renamed