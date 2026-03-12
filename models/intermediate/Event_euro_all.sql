SELECT 
    e.*,
    l.poste
FROM {{ ref('stg_Raw_data__Events_euro_2024') }} AS e
LEFT JOIN {{ ref('stg_Raw_data__Poste_euro_2024') }} AS l
ON e.player = l.player_name