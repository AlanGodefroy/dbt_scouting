WITH sq1 AS (

SELECT

    evl.player_id,

    evl.player,

    pl.poste,

    COUNT(DISTINCT(evl.match_id)) AS nb_matches,

    COUNTIF(evl.duel_outcome IN ('Success','Success in play','Won')) as nb_duel_outcome,

    COUNTIF(evl.interception_outcome IN ('Success','Success in play','Won')) as nb_interception_outcome,

    (COUNT(evl.block_deflection) + COUNT(evl.block_save_block)) as nb_block,

    COUNT(evl.clearance_aerial_won) as nb_clearance_aerial_won,

    COUNT(evl.under_pressure) as nb_under_pressure_succes,

    COUNTIF(evl.pass_outcome IS NULL AND evl.event_type = "Pass") as nb_pass_outcome_complete,

    COUNT(evl.pass_length) as nb_pass_length,

    ROUND(AVG(evl.pass_length),2) as avg_length_of_pass_length

    

   

FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS evl

LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS pl

    ON evl.player = pl.player_name

WHERE evl.player_id IS NOT NULL and pl.poste like 'Defense'

GROUP BY 1,2,3

ORDER BY 3

),



sq2 as (



SELECT sq1.*,

ROUND(SAFE_DIVIDE(sq1.nb_under_pressure_succes,sq1.nb_matches),2) as nb_under_pressure_succes_per_matches,

ROUND(SAFE_DIVIDE(sq1.nb_pass_outcome_complete,sq1.nb_matches),2) as nb_pass_outcome_complete_per_matches,

ROUND(SAFE_DIVIDE(sq1.nb_pass_length,sq1.nb_matches),2) as nb_pass_length_per_matches



FROM sq1 

)



SELECT sq2.*,

ROUND(SUM(coll.minutes_played),2) as minutes_played_total,

ROUND(SAFE_DIVIDE(SUM(coll.minutes_played),sq2.nb_matches),2) as minutes_played_per_matches







FROM sq2

LEFT JOIN {{ ref('int_collective_kpis') }} as coll

    ON sq2.player_id = coll.player_id

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15