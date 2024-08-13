import requests
import pandas as pd
import ibis
from ibis import _

def get_ffc_adp(format, year, teams="12", position="all"):
    # Construct the URL

    url = f"https://fantasyfootballcalculator.com/api/v1/adp/{format}?teams={teams}&year={year}&position={position}"

    # Fetch the data from the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON content
        json_data = response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

    # Convert the players data to a DataFrame and add the season
    players_df = pd.json_normalize(json_data["players"])
    players_df["season"] = year
    players_df["format"] = format

    return players_df


if __name__ == '__main__':
    con = ibis.duckdb.connect("data/luna.duckdb")
    try:
        df_adp_ppr = pd.concat(
            [get_ffc_adp(format="ppr", year=year) for year in range(2014, 2025)],
            ignore_index=True,
        )
        con.create_table("FFC_ADP_PPR", df_adp_ppr, overwrite=True, database="BASE")
        print("PPR ADP CREATED")
    except:
        print("PPR ADP ERROR")
    try:
        df_adp_2qb = pd.concat(
            [get_ffc_adp(format="2qb", year=year) for year in range(2014, 2025)],
            ignore_index=True,
        )
        con.create_table("FFC_ADP_2QB", df_adp_2qb, overwrite=True, database="BASE")
        print("2QB ADP CREATED")
    except:
        print("2QB ADP ERROR")
