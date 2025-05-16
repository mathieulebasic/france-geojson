import json
import os
from utils import query
import territories_list
import subprocess

def load_communes_geometries(territoire_type : str):
    sql = f"""
        select ctc.code_territoire, code_commune, libelle_commune, st_asgeojson(geometrie) as geometrie
        from dbt_api_territoires.tous_les_territoires tlt
        inner join dbt_api_territoires.correspondance_territoires_commune ctc using(code_territoire)
        left join territoires.communes_geometrie cg using(code_commune)
        left join territoires.communes c using(code_commune)
        where echelle_geographique = '{territoire_type}' and geometrie is not null
            AND code_region NOT IN ('01', '02', '03', '04', '06', 'COM')
    """
    df = query(sql)
    return df

def createGeoJson(df, nom_fichier, detail_level : str = 'com'):
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for index, row in df.iterrows():

        feature = {
            "type": "Feature",
            "geometry": json.loads(row['geometrie']),
            "properties": {
                "code_territoire": detail_level + row['code_commune'],
                "libelle_territoire": row['libelle_commune'],
            }
        }
        geojson["features"].append(feature)



    with open(nom_fichier, 'w') as f:
        json.dump(geojson, f)

    subprocess.run(['mapshaper', nom_fichier, '-simplify', 'dp', '10%', '-o','force',  'precision=0.00001', nom_fichier], check=True)


def create_geojson_files_for_territoire_type(geojson_level : str, territoire_type : str):
    df = load_communes_geometries(territoire_type)
    code_territoire_list = df['code_territoire'].unique()
    for code_territoire in code_territoire_list:
        print(f"Processing code_territoire: {code_territoire}")
        nom_fichier = os.path.join(geojson_level, f"{code_territoire}.geojson")
        df_filtered = df[df['code_territoire'] == code_territoire]
        createGeoJson(df_filtered, nom_fichier)
        print(f"GeoJSON file created: {nom_fichier}")

def create_all_geojson_files():
    for territoire_type in territories_list.INCLUDED_TERRITORIES:
        print(f"Creating GeoJSON files for {territoire_type}")
        create_geojson_files_for_territoire_type('communes', territoire_type)
        print(f"GeoJSON files for {territoire_type} created successfully.")

if __name__ == "__main__":
    create_all_geojson_files()
