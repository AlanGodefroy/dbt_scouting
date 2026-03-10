with 

source as (

    select * from {{ source('Raw_data', 'Mateches_euro_2024') }}

),

renamed as (

    select
        int64_field_0,
        match_id,
        match_date,
        kick_off,
        competition,
        season,
        home_team,
        away_team,
        home_score,
        away_score,
        match_status,
        match_status_360,
        last_updated,
        last_updated_360,
        match_week,
        competition_stage,
        stadium,
        referee,
        home_managers,
        away_managers,
        data_version,
        shot_fidelity_version,
        xy_fidelity_version

    from source

)

select * from renamed