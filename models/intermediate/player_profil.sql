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
),

-- Calcul des stats brutes par joueur
stats_brutes as (
    select
        player,
        poste,
        COUNT(DISTINCT match_id)                        AS matchs_joues,
        COUNTIF(shot_outcome IS NOT NULL)               AS nb_tirs,
        SUM(shot_statsbomb_xg)                          AS xG_total,
        COUNTIF(pass_goal_assist = TRUE)                AS nb_passes_decisives,
        COUNTIF(duel_type IS NOT NULL)                  AS nb_duels,
        COUNTIF(dribble_outcome = 'Complete')           AS nb_dribbles,
        COUNTIF(interception_outcome IS NOT NULL)       AS nb_interceptions,
        COUNTIF(dribble_outcome IS NOT NULL)            AS nb_tentatives_dribble

    from joined
    where team = 'Bayer Leverkusen'
    group by player, poste
),

-- Classification par comportement réel
profils as (
    select
        player,
        matchs_joues,
        nb_tirs,
        xG_total,
        nb_passes_decisives,
        nb_duels,
        nb_dribbles,
        nb_interceptions,
        poste,

        case
            -- Profil offensif : beaucoup de tirs et xG
            when nb_tirs > 20
             and xG_total > 3                           then 'Profil_Offensif'

            -- Profil créateur : passes décisives + dribbles
            when nb_passes_decisives > 3
             and nb_dribbles > 10                       then 'Profil_Createur'

            -- Profil défensif : duels + interceptions
            when nb_duels > 50
             and nb_interceptions > 5                   then 'Profil_Defensif'

            -- Profil box-to-box : mix offensif et défensif
            when nb_tirs > 10
             and nb_duels > 30                          then 'Profil_Box_to_Box'

            else 'Profil_Mixte'
        end AS profil_tactique

    from stats_brutes
)

select * from profils
order by xG_total DESC