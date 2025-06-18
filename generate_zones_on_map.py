import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx


shp_file_path = "data/zones_shape/taxi_zones.shp"
taxi_zones = gpd.read_file(shp_file_path)

taxi_zones_wm = taxi_zones.to_crs(epsg=3857)

fig, ax = plt.subplots(1, 1, figsize=(15, 15))

taxi_zones_wm.plot(
    ax=ax, color="lightgray", edgecolor="black", alpha=0.7, linewidth=0.5
)

cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, zoom=13)

for idx, row in taxi_zones_wm.iterrows():
    centroid = row.geometry.centroid
    location_id = row["LocationID"]

    ax.text(
        centroid.x,
        centroid.y,
        str(location_id),
        fontsize=6,
        ha="center",
        va="center",
        color="darkred",
        path_effects=[
            plt.matplotlib.patheffects.Stroke(linewidth=1.5, foreground="white"),
            plt.matplotlib.patheffects.Normal(),
        ],
    )

ax.set_axis_off()

output_image_path = "data/nyc_taxi_zones_map_with_ids.svg"
plt.savefig(output_image_path, dpi=300, bbox_inches="tight")

plt.show()
