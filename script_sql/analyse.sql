-- 0 
SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dt.annee,
    dl.type_local,
        CAST(
        CONCAT(
            dt.annee, '-',
            CASE WHEN dt.mois < 10 THEN '0' + CAST(dt.mois AS VARCHAR) ELSE CAST(dt.mois AS VARCHAR) END, '-',
            CASE WHEN dt.jour < 10 THEN '0' + CAST(dt.jour AS VARCHAR) ELSE CAST(dt.jour AS VARCHAR) END
        ) AS DATE
    ) AS date_transaction_f,
    COUNT(*) OVER () AS total_lignes
from facts_transactions_f ft 
JOIN dim_commune_f dc ON ft.id_commune = dc.id_commune
JOIN dim_mutation_f dm ON ft.id_mutation = dm.id_mutation
JOIN dim_local_f dl ON ft.id_local = dl.id_local
JOIN dim_temps_f dt ON ft.id_temps = dt.id_temps
WHERE dt.annee BETWEEN 2021 AND 2024;



-- 1. Analyse des prix moyens des transactions en fonction du types de bien et de l'année 
SELECT
    dt.annee,
    dl.type_local,
    AVG(ft.valeur_fonciere) AS valeur_fonciere_moyenne
FROM facts_transactions ft 
JOIN dim_temps dt ON ft.id_temps = dt.id_temps
JOIN dim_local dl ON ft.id_local = dl.id_local
GROUP BY dt.annee, dl.type_local
ORDER BY dt.annee DESC, valeur_fonciere_moyenne DESC;

-- 2. Top 10 des communes avec le plus grand volume de transaction en 2023
SELECT TOP 10
    dc.commune,
    COUNT(ft.valeur_fonciere) AS nombre_transaction
FROM facts_transactions ft 
JOIN dim_comune dc ON ft.id_commune = dc.id_commune
GROUP BY dc.commune
ORDER BY nombre_transaction DESC;

-- 3. Surface moyenne des biens vendus par type de bien
SELECT
    dl.type_local,
    AVG(ft.surface_reelle_bati) AS surface_moyenne
FROM facts_transactions ft
JOIN dim_local dl ON ft.id_local = dl.id_local
GROUP BY dl.type_local
ORDER BY surface_moyenne DESC;

-- 4. Valeur fonciere médiane par moi et par an
WITH medianvalues AS (
    SELECT
        dt.annee, 
        dt.mois,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.valeur_fonciere) 
        OVER (PARTITION BY dt.annee, dt.mois) AS valeur_fonciere_mediane
    FROM facts_transactions ft
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    --GROUP BY dt.annee, dt.mois 
)
SELECT DISTINCT annee, mois, valeur_fonciere_mediane
FROM medianvalues
ORDER BY annee DESC, mois DESC;

-- 5. Evolution du nombre de transaction par année
SELECT
    dt.annee,
    COUNT(ft.valeur_fonciere) AS nombre_transaction
FROM facts_transactions ft 
JOIN dim_temps dt ON ft.id_temps = dt.id_temps
GROUP BY dt.annee
ORDER BY dt.annee DESC;


-- 6. Prix moyen au m2 par type de bien avec classement
WITH prix_par_m2 AS(
    SELECT
        dl.type_local,
        dc.commune,
        dc.code_departement,
        -- FORMAT(CONCAT(dt.annee, '-', RIGHT('0' + CAST(dt.mois AS VARCHAR(2)), 2), '-', RIGHT('0' + CAST(dt.jour AS VARCHAR(2)), 2)), 'yyyy-MM-dd') AS date_transact,
        SUM(ft.valeur_fonciere) / NULLIF(SUM(ft.surface_reelle_bati), 0) AS prix_m2_moyen
    FROM facts_transactions ft 
    JOIN dim_local dl ON ft.id_local = dl.id_local
    JOIN dim_comune dc ON ft.id_commune = dc.id_commune
    -- JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    WHERE ft.surface_reelle_bati > 0
    GROUP BY dl.type_local, dc.commune, dc.code_departement
)
SELECT 
    type_local,
    commune, 
    code_departement,
    prix_m2_moyen,
    date_transact,
    RANK() OVER (ORDER BY prix_m2_moyen DESC) AS rang
FROM prix_par_m2;

-- 7. Transactions les plus chères par commune en 2023
WITH ranked_transactions AS (
    SELECT 
        dc.commune,
        ft.valeur_fonciere,
        ROW_NUMBER() OVER (PARTITION BY dc.commune ORDER BY ft.valeur_fonciere DESC) AS rang
    FROM facts_transactions ft 
    JOIN dim_comune dc ON ft.id_commune = dc.id_commune
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    WHERE dt.annee = 2023
)
SELECT commune, valeur_fonciere
FROM ranked_transactions
WHERE rang = 1
ORDER BY valeur_fonciere DESC;

-- 8. Evolution de la plart des ventes des maisons par rapport aux appartements 
WITH total_transactions AS (
    SELECT 
        dt.annee,
        dl.type_local,
        COUNT(ft.valeur_fonciere) AS nombre_transactions
    FROM facts_transactions ft 
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    JOIN dim_local dl ON ft.id_local = dl.id_local
    WHERE dl.type_local IN ('Maison', 'Appartement')
    GROUP BY dt.annee, dl.type_local
)
SELECT
    annee,
    type_local,
    nombre_transactions,
    SUM(nombre_transactions) OVER (PARTITION BY annee) AS total_anne,
    CAST(nombre_transactions AS FLOAT) / SUM (nombre_transactions) OVER (PARTITION BY annee) AS part_marche
FROM total_transactions
ORDER BY annee DESC, part_marche DESC;

-- 9. Prix moyen des transactions par département avec comparaison du prix poyen nationale
SELECT
    dc.code_departement,
    AVG(ft.valeur_fonciere) AS prix_moyen_departement,
    AVG(AVG(ft.valeur_fonciere)) OVER () AS prix_moyen_national,
    AVG(ft.valeur_fonciere) - AVG(AVG(ft.valeur_fonciere)) OVER () AS ecart_moyenne
FROM facts_transactions ft 
JOIN dim_comune dc ON ft.id_commune = dc.id_commune
GROUP BY dc.code_departement
ORDER BY ecart_moyenne DESC;

-- 10 Tendance des prix par type de bien 
WITH prix_trend AS (
    SELECT 
        dt.annee,
        dl.type_local,
        AVG(ft.valeur_fonciere) AS prix_moy
    FROM facts_transactions ft 
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    JOIN dim_local dl ON ft.id_local = dl.id_local
    GROUP BY dt.annee, dl.type_local
)
SELECT
    annee,
    type_local,
    prix_moy,
    AVG(prix_moy) OVER (PARTITION BY type_local ORDER BY annee ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moyenne_mobile_3_ans
FROM prix_trend
ORDER BY type_local, annee DESC;

-- 11. Classement des communes selon leur attractivité immobilière 
WITH transactions_ranks AS (
    SELECT
        dc.commune,
        dt.annee,
        COUNT(ft.valeur_fonciere) AS nombre_transactions_imo,
        SUM(ft.valeur_fonciere) AS valeur_fonciere_total
    FROM facts_transactions ft 
    JOIN dim_comune dc ON ft.id_commune = dc.id_commune
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    GROUP BY dc.commune, dt.annee
)
SELECT
    commune,
    annee,
    nombre_transactions_imo,
    valeur_fonciere_total,
    RANK() OVER (ORDER BY nombre_transactions_imo DESC) AS rang_transactions_imo,
    RANK() OVER (ORDER BY valeur_fonciere_total DESC) AS rang_valeur,
    (RANK() OVER (ORDER BY nombre_transactions_imo DESC) + RANK() OVER (ORDER BY valeur_fonciere_total DESC)) / 2 AS scrore_attractivite
FROM transactions_ranks
ORDER BY scrore_attractivite ASC;

-- 12. Transaction record par année (top 5 ventes par annee)
WITH top_transactions AS (
    SELECT 
        dt.annee,
        ft.valeur_fonciere,
        dc.commune,
        dl.type_local,
        ROW_NUMBER() OVER (PARTITION BY dt.annee ORDER BY ft.valeur_fonciere DESC) AS rang
    FROM facts_transactions ft 
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    JOIN dim_comune dc ON ft.id_commune = dc.id_commune
    JOIN dim_local dl ON ft.id_local = dl.id_local
)
SELECT annee, commune, type_local, valeur_fonciere
FROM top_transactions
WHERE rang <= 5
ORDER BY annee DESC, valeur_fonciere DESC;

-- 12 Taux d'augementation ou de baisse des prix par département
WITH prix_par_annee AS (
    SELECT
        dt.annee,
        dc.code_departement,
        AVG(ft.valeur_fonciere) AS prix_moyn
    FROM facts_transactions ft
    JOIN dim_temps dt ON ft.id_temps = dt.id_temps
    JOIN dim_comune dc ON ft.id_commune = dc.id_commune
    GROUP BY dt.annee, dc.code_departement
)
SELECT
    annee,
    code_departement,
    prix_moyn,
    LAG(prix_moyn) OVER (PARTITION BY code_departement ORDER BY annee) AS prix_annee_precd,
    (prix_moyn - LAG(prix_moyn) OVER (PARTITION BY code_departement ORDER BY annee)) / NULLIF(LAG(prix_moyn) OVER (PARTITION BY code_departement ORDER BY annee), 0) AS taux_variation
FROM prix_par_annee
ORDER BY code_departement, annee DESC;


SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dl.type_local,
    dt.annee,
        CAST(
        CONCAT(
            dt.annee, '-',
            CASE WHEN dt.mois < 10 THEN '0' + CAST(dt.mois AS VARCHAR) ELSE CAST(dt.mois AS VARCHAR) END, '-',
            CASE WHEN dt.jour < 10 THEN '0' + CAST(dt.jour AS VARCHAR) ELSE CAST(dt.jour AS VARCHAR) END
        ) AS DATE
    ) AS date_transaction,
    COUNT(*) OVER () AS nombre_de_lignes
from facts_transactions_f ft 
JOIN dim_temps_f dt ON ft.id_temps = dt.id_temps
JOIN dim_commune_f dc ON ft.id_commune = dc.id_commune
JOIN dim_mutation_f dm ON ft.id_mutation = dm.id_mutation
INNER JOIN dim_local_f dl ON ft.id_local = dl.id_local
WHERE dt.annee BETWEEN 2021 AND 2024;


-- Op
SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dl.type_local,
    dt.annee,
    CAST(
        CONCAT(
            dt.annee, '-',
            CASE WHEN dt.mois < 10 THEN '0' + CAST(dt.mois AS VARCHAR) ELSE CAST(dt.mois AS VARCHAR) END, '-',
            CASE WHEN dt.jour < 10 THEN '0' + CAST(dt.jour AS VARCHAR) ELSE CAST(dt.jour AS VARCHAR) END
        ) AS DATE
    ) AS date_transaction
FROM facts_transactions_f ft
JOIN dim_temps_f dt ON ft.id_temps = dt.id_temps
JOIN dim_commune_f dc ON ft.id_commune = dc.id_commune
JOIN dim_mutation_f dm ON ft.id_mutation = dm.id_mutation
LEFT JOIN dim_local_f dl ON ft.id_local = dl.id_local
WHERE dt.annee BETWEEN 2023 AND 2024;





SELECT COUNT(*) AS nombre_de_lignes
FROM facts_trans_11 ft 
JOIN dim_commune_11 dc ON ft.id_commune = dc.id_commune
JOIN dim_mutation_11 dm ON ft.id_mutation = dm.id_mutation
JOIN dim_local_11 dl ON ft.id_local = dl.id_local
JOIN dim_temps_11 dt ON ft.id_temps = dt.id_temps;


SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dl.type_local,
    CAST(
        CONCAT(
            dt.annee, '-',
            RIGHT('00' + CAST(dt.mois AS VARCHAR), 2), '-',
            RIGHT('00' + CAST(dt.jour AS VARCHAR), 2)
        ) AS DATE
    ) AS date_transaction,
    COUNT(*) OVER () AS total_lignes
FROM facts_trans_11 ft
LEFT JOIN dim_commune_11 dc ON ft.id_commune = dc.id_commune
LEFT JOIN dim_mutation_11 dm ON ft.id_mutation = dm.id_mutation
LEFT JOIN dim_local_11 dl ON ft.id_local = dl.id_local
LEFT JOIN dim_temps_11 dt ON ft.id_temps = dt.id_temps
ORDER BY date_transaction;


SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dl.type_local,
    dt.annee,
    CAST(
        CONCAT(
            RIGHT('0000' + CAST(dt.annee AS VARCHAR), 4), '-',
            RIGHT('00' + CAST(dt.mois AS VARCHAR), 2), '-',
            RIGHT('00' + CAST(dt.jour AS VARCHAR), 2)
        ) AS DATE
    ) AS date_transaction,
    COUNT(*) OVER () AS nombre_de_lignes
FROM facts_trans_11 ft 
LEFT JOIN dim_commune_11 dc ON ft.id_commune = dc.id_commune
LEFT JOIN dim_mutation_11 dm ON ft.id_mutation = dm.id_mutation
LEFT JOIN dim_local_11 dl ON ft.id_local = dl.id_local
LEFT JOIN dim_temps_11 dt ON ft.id_temps = dt.id_temps
ORDER BY dt.annee, date_transaction;


SELECT
    ft.valeur_fonciere,
    ft.surface_reelle_bati,
    ft.nombre_pieces_principales,
    dc.commune,
    dc.code_departement,
    dm.nature_mutation,
    dl.type_local,
    dt.annee,
    CAST(
        CONCAT(
            RIGHT('0000' + CAST(dt.annee AS VARCHAR), 4), '-',
            RIGHT('00' + CAST(dt.mois AS VARCHAR), 2), '-',
            RIGHT('00' + CAST(dt.jour AS VARCHAR), 2)
        ) AS DATE
    ) AS date_transaction,
    COUNT(*) OVER () AS nombre_de_lignes
FROM facts_trans_11 ft 
LEFT JOIN dim_commune_11 dc ON ft.id_commune = dc.id_commune
LEFT JOIN dim_mutation_11 dm ON ft.id_mutation = dm.id_mutation
LEFT JOIN dim_local_11 dl ON ft.id_local = dl.id_local
FULL OUTER JOIN dim_temps_11 dt ON ft.id_temps = dt.id_temps
ORDER BY dt.annee, date_transaction;


SELECT
    ft.valeur_fonciere,
    --ft.surface_reelle_bati,
    --ft.nombre_pieces_principales,
   -- dc.commune,
    dc.code_departement,
    dm.nature_mutation,
     dl.type_local,
        CAST(
        CONCAT(
            dt.annee, '-',
            CASE WHEN dt.mois < 10 THEN '0' + CAST(dt.mois AS VARCHAR) ELSE CAST(dt.mois AS VARCHAR) END, '-',
            CASE WHEN dt.jour < 10 THEN '0' + CAST(dt.jour AS VARCHAR) ELSE CAST(dt.jour AS VARCHAR) END
        ) AS DATE
    ) AS date_transaction,
    COUNT(*) OVER () AS nombre_de_lignes
from   dim_temps_f dt 
JOIN  facts_transactions_f ft ON ft.id_temps = dt.id_temps
JOIN dim_commune_f dc ON ft.id_commune = dc.id_commune 
JOIN dim_mutation_f dm ON ft.id_mutation = dm.id_mutation
JOIN dim_local_f dl ON ft.id_local = dl.id_local

SELECT 

COUNT(*) OVER () AS nombre_de_lignes
FROM facts_transactions_f
