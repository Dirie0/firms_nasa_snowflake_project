"""One-off: build the country polygons and the two dimension tables, and upload them to S3."""

from io import BytesIO

import geopandas as gpd
import pandas as pd
import requests

import config
import utils

SOURCE_URL = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"


def upload(key, table):
    utils.s3_put_csv(key, table)
    print(f"Wrote {len(table)} rows to {key}")


def main():
    response = requests.get(SOURCE_URL, timeout=120)
    response.raise_for_status()
    shapes = gpd.read_file(BytesIO(response.content)).to_crs("EPSG:4326")

    iso3 = shapes["ISO_A3_EH"].where(shapes["ISO_A3_EH"] != "-99", shapes["ADM0_A3"])

    countries = pd.DataFrame({
        "country_iso3": iso3,
        "country_name": shapes["NAME"],
        "continent": shapes["CONTINENT"],
        "geometry_wkt": shapes.geometry.to_wkt(),
    })

    unknown_country = pd.DataFrame([config.UNKNOWN_COUNTRY])
    dim_country = pd.concat([unknown_country, countries[config.DIM_COUNTRY_COLUMNS]], ignore_index=True)
    dim_country = dim_country.drop_duplicates(subset="country_iso3")

    dim_satellite = pd.DataFrame(
        [{"satellite": code, **satellite} for code, satellite in config.SATELLITES.items()]
    )[config.DIM_SATELLITE_COLUMNS]

    upload(config.COUNTRIES_KEY, countries)
    upload(config.DIM_COUNTRY_KEY, dim_country)
    upload(config.DIM_SATELLITE_KEY, dim_satellite)


if __name__ == "__main__":
    main()