SELECT 
    o.*,
    p.poste
FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS o
LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS p
ON o.player = p.player_name