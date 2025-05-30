import json
import os
from utils import query
from geometry_utils import createGeoJson
import territories_list


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

def load_iris_geometries(territoire_type : str):
    sql = f"""
        select ctc.code_territoire, code_iris, libelle_iris, st_asgeojson(geometrie) as geometrie
        from dbt_api_territoires.tous_les_territoires tlt
        inner join dbt_api_territoires.correspondance_territoires_commune ctc using(code_territoire)
        left join territoires.communes c using(code_commune)
        left join territoires.iris i using(code_commune)
        left join territoires.iris_geometrie ig using(code_iris)
        where echelle_geographique = '{territoire_type}' and geometrie is not null
            AND code_region NOT IN ('01', '02', '03', '04', '06', 'COM')
    """
    df = query(sql)
    return df

def load_epci_geometries(territoire_type : str):
    sql = f"""
        select DISTINCT ctc.code_territoire, code_epci, libelle_epci, st_asgeojson(geometrie) as geometrie
        from dbt_api_territoires.tous_les_territoires tlt
        inner join dbt_api_territoires.correspondance_territoires_commune ctc using(code_territoire)
        left join territoires.communes c using(code_commune)
        left join territoires.epci i using(code_epci)
        left join territoires.epci_geometrie eg using(code_epci)
        where echelle_geographique = '{territoire_type}' and geometrie is not null
            AND code_region NOT IN ('01', '02', '03', '04', '06', 'COM')
    """
    df = query(sql)
    return df

def load_specific_territories_geometries(geojson_level, parent_type : str):
    if geojson_level == 'iris':
        return load_iris_geometries(parent_type)
    elif geojson_level == 'epci':
        return load_epci_geometries(parent_type)
    else:
        return load_communes_geometries(parent_type)

def create_geojson_files_for_territoire_type(geojson_level : str, parent_type : str):
    df = load_specific_territories_geometries(geojson_level, parent_type)
    code_territoire_list = df['code_territoire'].unique()

    for code_territoire in code_territoire_list:
        print(f"Processing code_territoire: {code_territoire}")
        nom_fichier = os.path.join(geojson_level, f"{code_territoire}.geojson")
        df_filtered = df[df['code_territoire'] == code_territoire]
        createGeoJson(df_filtered, nom_fichier, geojson_lebel=geojson_level)
        print(f"GeoJSON file created: {nom_fichier}")

def create_all_geojson_files(geojson_level):
    for territoire_type in territories_list.INCLUDED_TERRITORIES:
        print(f"Creating GeoJSON files for {territoire_type}")
        create_geojson_files_for_territoire_type(geojson_level, territoire_type)
        print(f"GeoJSON files for {territoire_type} created successfully.")

if __name__ == "__main__":
    create_all_geojson_files('iris')
