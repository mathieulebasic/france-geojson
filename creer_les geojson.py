import pandas as pd
import json
from sqlalchemy import create_engine
from pathlib import Path
import os
import pandas.io.sql as sqlio
import sqlalchemy
import ast
NOM_FICHIER = "pnr_geojsons.csv"
NOM_DOSSIER = "departements_epci"
#geojson_df = pd.read_csv(NOM_FICHIER)

def chargerIdentifiants(fichier="identifiants_azure.json"):
    if fichier == None:
        fichier = "identifiants_azure.json"
    with open(os.path.join(Path(__file__).parents[2], 'oad', fichier)) as f:
        return json.load(f)

def creerConnectionOAD(fichier="identifiants_azure.json"):
    identifiants = chargerIdentifiants(fichier)["postgresql"]
    engine = create_engine(
        "postgresql://"
        + identifiants["user"]
        + ":"
        + identifiants["password"]
        + "@"
        + identifiants["host"]
        + ":"
        + identifiants["port"]
        + "/"
        + identifiants["database"]
        , future=True #mis ca pour le RA sinon il y avait une erreur sur la connection
    )
    return engine.connect()

def query(sql, sql_params = None):
    connection = creerConnectionOAD()
    return sqlio.read_sql_query(sqlalchemy.text(sql), connection, params=sql_params)

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

def creerConnectionOAD(fichier="identifiants_azure.json"):
    identifiants = chargerIdentifiants(fichier)["postgresql"]
    engine = create_engine(
        "postgresql://"
        + identifiants["user"]
        + ":"
        + identifiants["password"]
        + "@"
        + identifiants["host"]
        + ":"
        + identifiants["port"]
        + "/"
        + identifiants["database"]
        , future=True #mis ca pour le RA sinon il y avait une erreur sur la connection
    )
    return engine.connect()

geo_query = "SELECT * FROM dbt_territoires.departements_epci_geometries"
geojson_df = query(geo_query, sql_params={"dtypes": {"code_departement": sqlalchemy.types.String, "geojson": sqlalchemy.types.JSON}})
geojson_df["geojson"] = geojson_df["geojson"].apply(lambda x: str(x))

print("")
for index, row in geojson_df.iterrows():
    geojson = row["geojson"]
    nom_fichier_geojson = row["code_departement"]
    geojson = ast.literal_eval(row["geojson"])
    print(nom_fichier_geojson)


    with open(f"{NOM_DOSSIER}/{nom_fichier_geojson}.geojson", "w") as f:
        json.dump(geojson, f)
