import geopandas as gpd
import matplotlib.pyplot as plt

# 1. Load the shapefile
shp_path = "/home/shemking/Downloads/Kenya_Wards"  # Update with your actual path
gdf = gpd.read_file(shp_path)

# 2. Inspect the shapefile structure
print("Columns in the shapefile:")
print(gdf.columns)

print("\nFirst 5 records:")
print(gdf.head())

print("\nCoordinate Reference System (CRS):")
print(gdf.crs)

# Print column names
print("Column Names:\n", gdf.columns.tolist())

# Print data types of each column
print("\nData Types:\n", gdf.dtypes)

# Print first 5 records to inspect content
print("\nSample Records:\n", gdf.head())
# 3. Quick plot to visualize the shape
gdf.plot(figsize=(8, 6))
plt.title("Shapefile Preview")
plt.show()
