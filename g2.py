from datetime import datetime, timedelta
import random
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, avg, to_timestamp

stations = ["Gare du Nord", "Châtelet", "Montparnasse", "Nation", "Opéra"]
lines = ("L1", "L2", "L3","L4")

def ma_fonction():
    records = []
    start = datetime(2024, 1, 1)
    jours = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

    for i in range(500_000):
        dt = start + timedelta(minutes=random.randint(0, 525_600))

        record = {
             
            "station": random.choice(stations),
            "datetime": dt.strftime("%Y-%m-%d %H:%M"),
            "passengers": int(np.random.poisson(lam=80)), 
            "lines": random.choice(lines),}
        mon_dict = {
    "jours": random.choice(jours)
}  

        records.append(record)

    print("Sample:", records[0])  

    df = pd.DataFrame(records)
    df.to_csv("transport_data.csv", index=False)
    
    print("Dataset generated successfully")
ma_fonction()
def ma_fonction0():
       df = pd.read_csv("transport_data.csv", parse_dates=["datetime"])

       df["datetime"] = pd.to_datetime(df["datetime"])
       df["year"] = df["datetime"].dt.year
       df["month"] = df["datetime"].dt.month
       df["day_name"] = df["datetime"].dt.day_name()

ma_fonction0()
def ma_fonction2():
    spark = SparkSession.builder.appName("TransportAnalysis").getOrCreate()

    # Charger les données
    df = spark.read.csv(
        r"C:\Users\pc\Desktop\projet stations\transport_data.csv",
        header=True,
        inferSchema=True
    )

    # Convertir datetime
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm"))

    # Ajouter des features temporelles
    df = df.withColumn("hour", hour(col("datetime"))) \
           .withColumn("day", dayofweek(col("datetime")))

    # Nettoyer
    df = df.dropna().dropDuplicates()

    # Analyse
    peak = df.groupBy("station", "hour") \
             .agg(avg("passengers").alias("avg_passengers")) \
             .orderBy("avg_passengers", ascending=False)

    peak.show(10)

    return spark, df   # retourne spark et df
    spark = SparkSession.builder.appName("TransportAnalysis").getOrCreate()
    ...
    return df  

# Appel de la fonction
spark,df = ma_fonction2()

# Créer la table SQL temporaire
df.createOrReplaceTempView("transport")

# Top 5 stations 
print ("top 5 busiest stations")
top_stations = spark.sql("""
    SELECT station, SUM(passengers) AS total_passengers
    FROM transport
    GROUP BY station
    ORDER BY total_passengers DESC
    LIMIT 5
""")
top_stations.show()

# Heures de pointe
print ("top heures les plus chargées")
peak_hours = spark.sql("""
    SELECT hour, SUM(passengers) AS total_passengers
    FROM transport
    WHERE hour BETWEEN 7 AND 9 OR hour BETWEEN 17 AND 19
    GROUP BY hour
    ORDER BY total_passengers DESC
""")
peak_hours.show()
# Charger le CSV généré
df = pd.read_csv(r"C:\Users\pc\Desktop\projet stations\transport_data.csv", parse_dates=["datetime"])

# Extraire les variables temporelles
df["hour"] = df["datetime"].dt.hour

df["dayofweek"] = df["datetime"].dt.day_name(locale="fr_FR")
# Diagramme en barres du trafic horaire
trafic_horaire = df.groupby("hour")["passengers"].sum()
plt.figure(figsize=(10,6))
trafic_horaire.plot(kind="bar", color="steelblue")
plt.title("Trafic horaire")
plt.xlabel("Heure de la journée")
plt.ylabel("Nombre total de passagers")
plt.tight_layout()
plt.savefig("trafic_horaire.png")
plt.close()

# Carte thermique (station vs heure)
station_heure = df.groupby(["station","hour"])["passengers"].sum().unstack(fill_value=0)
plt.figure(figsize=(12,8))
sns.heatmap(station_heure, cmap="YlGnBu")
plt.title("Carte thermique : station vs hour")
plt.xlabel("Hour")
plt.ylabel("Station")
plt.tight_layout()
plt.savefig("station_heure_heatmap.png")
plt.close()
# Regroupement par jour de la semaine
trafic_hebdo = df.groupby("dayofweek")["passengers"].sum()
ordre_jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
trafic_hebdo = trafic_hebdo.reindex(ordre_jours).fillna(0)  # éviter NaN

# Création de la figure
plt.figure(figsize=(10,6))

# Courbe principale avec index numérique
plt.plot(range(len(trafic_hebdo)), trafic_hebdo.values, marker="o", color="orange", label="Trafic")
plt.xticks(range(len(trafic_hebdo)), trafic_hebdo.index, rotation=45)

# Mise en évidence du jour de plus forte affluence
max_day = trafic_hebdo.idxmax()
max_val = trafic_hebdo.max()
"""trafic_hebdo.index.get_loc(max_day),  # Coordonnée X
    max_val,                              # Coordonnée Y
    color="red",                          # Couleur du point
    s=120,                                # Taille du point
    zorder=5,                             # Superposition (au-dessus des autres éléments)
    label="Pic de fréquentation"          # Légende
"""
plt.scatter(trafic_hebdo.index.get_loc(max_day), max_val, color="red", s=120, zorder=5, label="Pic de fréquentation")

# Ligne de moyenne
plt.axhline(trafic_hebdo.mean(), color="blue", linestyle="--", label="Moyenne")

# Titres et axes
plt.title("Fréquentation hebdomadaire")
plt.xlabel("Jour de la semaine")
plt.ylabel("Nombre total de passagers")
plt.grid(True)
plt.legend()

# Annotation des valeurs
for i, val in enumerate(trafic_hebdo):
    plt.text(i, val + (trafic_hebdo.max()*0.02), str(int(val)), ha="center", va="bottom")

# Ajustement et sauvegarde
plt.tight_layout()
plt.savefig("frequentation_hebdomadaire.png")
plt.close()

# Extraction du jour de la semaine 
df["dayofweek"] = df["datetime"].dt.day_name(locale="fr_FR")

# Comptage par jour de la semaine
day_counts = df["dayofweek"].value_counts()

# Réorganisation dans l'ordre des jours
jours_ordre = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
day_counts = day_counts.reindex(jours_ordre).fillna(0)

# Tracé de la courbe
plt.figure(figsize=(8, 5))
plt.plot(day_counts.index, day_counts.values, marker='o', linestyle='-', color='green')
plt.title("Nombre d'occurrences par jour de la semaine")
plt.xlabel("Jour de la semaine")
plt.ylabel("Nombre d'occurrences")
plt.grid(True)
plt.show()

