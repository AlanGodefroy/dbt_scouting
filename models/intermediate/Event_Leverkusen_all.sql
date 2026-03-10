SELECT 
    e.*,
    l.poste
FROM {{ ref('stg_Raw_data__Events_Leverkusen') }} AS e
LEFT JOIN {{ ref('stg_Raw_data__Poste_Leverkusen') }} AS l
ON e.player = l.player_name


