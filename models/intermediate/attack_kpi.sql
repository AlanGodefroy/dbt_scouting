with events as (
    select * from {{ ref('stg_Raw_data__Events_Leverkusen') }}
),

poste as (
    select * from {{ ref('stg_Raw_data__Poste_Leverkusen') }}
),

joined as (
    select
        events.*,
        poste.poste
    from events
    left join poste
        on events.player = poste.player_name
),  -- ✅ virgule manquante ici

kpi as (
    select
        player,
        poste,

        -- Buts
        COUNTIF(shot_outcome = 'Goal')                        AS buts,

        -- xG total
        ROUND(SUM(shot_statsbomb_xg), 2)                     AS xG_total,

        -- Buts vs xG (surperformance)
        ROUND(COUNTIF(shot_outcome = 'Goal') 
              - SUM(shot_statsbomb_xg), 2)                   AS buts_vs_xG,

        -- Tirs cadrés
        COUNTIF(shot_outcome IN ('Goal', 'Saved'))           AS tirs_cadres,

        -- Tirs en un touch
        COUNTIF(shot_first_time = TRUE)                      AS tirs_first_time,

        -- Tirs en 1v1
        COUNTIF(shot_one_on_one = TRUE)                      AS tirs_1v1,

        -- Dribbles réussis
        COUNTIF(dribble_outcome = 'Complete')                AS dribbles_reussis,

        -- Passes décisives
        COUNTIF(pass_goal_assist = TRUE)                     AS passes_decisives

    from joined
    where team = 'Bayer Leverkusen' and poste = 'Attack'
    group by player, poste
    order by xG_total DESC
)

select * from kpi