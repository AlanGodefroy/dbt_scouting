WITH poste_adjust as (

SELECT  
        player_name,
        attack,
        middle,
        defense,
        goal,
        poste as ancien_poste,
        CASE 
            WHEN player_name = 'Nathan Tella' THEN 'Attack'
        ELSE poste 
        END AS poste
        
FROM {{ source('Raw_data', 'Poste_Leverkusen') }}
)

SELECT 
        player_name,
        attack,
        middle,
        defense,
        goal,
        poste 
        
FROM poste_adjust