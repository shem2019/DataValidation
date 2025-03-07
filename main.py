import pandas as pd
import requests
import time

# Geo libraries for shapefile logic
import geopandas as gpd
from shapely.geometry import Point

# ======================================
# 1. Load CSV & Basic Setup
# ======================================
file_path = "Sites.csv"  # Update with your actual file path
df = pd.read_csv(file_path, encoding="ISO-8859-1")

API_KEY = ""  # Replace with your actual Google Maps API key
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
REVERSE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

MAX_REQUESTS = 5
request_count = 0

# Dictionary is optional if you still want that fallback logic:
kenya_administrative_structure = {
    "Mombasa": {
        "sub_counties": {
            "Changamwe": ["Port Reitz", "Kipevu", "Airport", "Chaani", "Changamwe"],
            # ... etc.
        }
    },
    # ... other counties ...
}

def get_constituency_ward(county, ward):
    """
    Dictionary-based approach: if 'ward' is recognized, find subcounty & ward name.
    """
    if county in kenya_administrative_structure:
        subcounties_dict = kenya_administrative_structure[county].get("sub_counties", {})
        for subc, wards in subcounties_dict.items():
            if ward in wards:
                return subc, ward
    return "Not Found", "Not Found"

# ======================================
# 2. Reverse Geocoding
# ======================================
def reverse_geocode(lat, lng):
    """
    Reverse geocode lat,lng -> (county, ward).
    We'll try to parse out administrative_area_level_2 (County), level_3 (Ward/Subcounty).
    """
    params = {
        "latlng": f"{lat},{lng}",
        "key": API_KEY
    }
    try:
        response = requests.get(REVERSE_GEOCODE_URL, params=params, timeout=10)
        data = response.json()
        if data["status"] == "OK":
            components = data["results"][0]["address_components"]
            found_county, found_ward = None, None
            for comp in components:
                types_ = comp.get("types", [])
                if "administrative_area_level_2" in types_:
                    found_county = comp["long_name"]
                if "administrative_area_level_3" in types_:
                    found_ward = comp["long_name"]
            return found_county or "Not Found", found_ward or "Not Found"
        else:
            return "Not Found", "Not Found"
    except requests.exceptions.RequestException:
        return "Error", "Error"
    except Exception:
        return "Error", "Error"

# Track changes
changed_rows = []

# ======================================
# 3. Fill Missing Data (Reverse Geocode + Dict)
# ======================================
def safe_apply(row):
    """
    If 'CONSTITUENCY' is missing, we parse SITECOORDINATES -> (lat, lng)
    Then reverse geocode to get (county, ward),
    Then dictionary to get subcounty, ward.
    """
    original_values = row[["CONSTITUENCY", "WARD", "SITECOORDINATES"]].tolist()

    if pd.isna(row["CONSTITUENCY"]):
        coords = row["SITECOORDINATES"]
        lat, lng = coords.split(",") if isinstance(coords, str) else (None, None)
        if lat and lng:
            county, ward = reverse_geocode(lat, lng)
            subcounty, matched_ward = get_constituency_ward(county, ward)
        else:
            subcounty, matched_ward = "Not Found", "Not Found"

        new_values = [subcounty, matched_ward, row["SITECOORDINATES"]]
        if new_values != original_values:
            changed_rows.append(row.name)
        return pd.Series(new_values + ["Updated"])
    else:
        # If CONSTITUENCY is not missing, we skip
        return row[["CONSTITUENCY", "WARD", "SITECOORDINATES"]].tolist() + ["Already Filled"]

df[["CONSTITUENCY", "WARD", "SITECOORDINATES", "STATUS"]] = df.apply(
    safe_apply, axis=1, result_type='expand'
)

# Save partial updates
df.to_csv("updated_sites_partial.csv", index=False, encoding="ISO-8859-1")

print("\n--- Reverse Geocode/Dictionary Step Done ---")
print(f"Rows updated: {changed_rows}")

# ======================================
# 4. GeoPandas Step: Use Shapefile to Fill subcounty, ward
# ======================================
# Suppose your shapefile has columns: ["gid", "pop2009", "county", "subcounty", "ward", "uid", "geometry"]

SHAPEFILE_PATH = "/home/shemking/Downloads/Kenya_Wards"  # ensure .dbf, .shx, etc. are alongside

print("\n--- Loading Shapefile for Spatial Join ---")
import geopandas as gpd
from shapely.geometry import Point

try:
    wards_gdf = gpd.read_file(SHAPEFILE_PATH)
    # Reproject to EPSG:4326 if needed
    wards_gdf = wards_gdf.to_crs(epsg=4326)
except Exception as e:
    print(f"Error loading shapefile: {e}")
    wards_gdf = None

if wards_gdf is not None:
    # Convert CSV df -> GeoDataFrame
    def parse_coords(coord_str):
        if isinstance(coord_str, str) and "," in coord_str:
            try:
                lat_str, lng_str = coord_str.split(",")
                lat, lng = float(lat_str), float(lng_str)
                return Point(lng, lat)  # note order: (x=lng, y=lat)
            except:
                return None
        return None

    df["geometry"] = df["SITECOORDINATES"].apply(parse_coords)
    gdf_sites = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Spatial join: which ward polygon is each site in?
    joined = gpd.sjoin(gdf_sites, wards_gdf, how="left", predicate="within")

    # The shapefile has columns: 'county', 'subcounty', 'ward', ...
    # Let's fill them into our final data if missing
    def fill_from_shapefile(row):
        # We check if the shapefile gave us better data
        # e.g. shapefile columns are "county", "subcounty", "ward"
        orig_subcounty = row["CONSTITUENCY"]  # in your CSV code, you're calling it 'CONSTITUENCY'
        orig_ward = row["WARD"]

        shape_subcounty = row.get("subcounty", None)
        shape_ward = row.get("ward", None)

        final_subcounty = orig_subcounty
        final_ward = orig_ward

        # If original is missing or 'Not Found', override with shapefile
        if not final_subcounty or final_subcounty.lower() == "not found":
            if isinstance(shape_subcounty, str):
                final_subcounty = shape_subcounty

        if not final_ward or final_ward.lower() == "not found":
            if isinstance(shape_ward, str):
                final_ward = shape_ward

        return pd.Series([final_subcounty, final_ward])

    joined[["FINAL_CONSTITUENCY", "FINAL_WARD"]] = joined.apply(fill_from_shapefile, axis=1)

    # Drop geometry columns
    joined.drop(columns=["geometry", "index_right"], inplace=True, errors="ignore")

    # Overwrite columns in joined DataFrame
    joined["CONSTITUENCY"] = joined["FINAL_CONSTITUENCY"]
    joined["WARD"] = joined["FINAL_WARD"]

    # Save final
    joined.to_csv("updated_sites.csv", index=False, encoding="ISO-8859-1")

    print("\n--- Shapefile Join Step Done ---")
    print(f"Final CSV saved as 'updated_sites.csv'")
    print("Columns in final data:", joined.columns.tolist())
else:
    print("\nNo shapefile updates were applied (could not load shapefile).")
