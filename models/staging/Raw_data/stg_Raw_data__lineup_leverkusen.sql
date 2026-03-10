with 

source as (

    select * from {{ source('Raw_data', 'lineup_leverkuseng') }}

),

renamed as (

    select

    from source

)

select * from renamed