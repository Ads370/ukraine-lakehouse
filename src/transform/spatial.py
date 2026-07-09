"""
transform/spatial.py: point-in-polygon assignment of oblast to lat/lon rows.
Uses shapely's STRtree for a fast vectorised spatial index. Reads the HDX
geoBoundaries ADM1 GeoJSON directly (stdlib json, no GDAL needed).

Note on shapely's STRtree.query: the predicate is applied from the INPUT
geometry's side, i.e. point.intersects(polygon). That's why
'intersects' (from the point) it's used rather than 'covers' (from the polygon).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from shapely import STRtree
from shapely.geometry import shape


class Boundaries:
    def __init__(self, geojson_path: str):
        with open(geojson_path) as f:
            data = json.load(f)
        self.geoms = []
        self.names: list[str] = []
        self.pcodes: list[str] = []
        for feat in data["features"]:
            props = feat["properties"]
            self.geoms.append(shape(feat["geometry"]))
            self.names.append(props.get("shapeName"))
            self.pcodes.append(props.get("shapeISO"))
        self.tree = STRtree(self.geoms)
        print(f"[spatial] loaded {len(self.geoms)} ADM1 polygons")

    def assign(self, df: pd.DataFrame, lon_col: str, lat_col: str) -> pd.DataFrame:
        """Return df with oblast_name and oblast_pcode columns added.

        Points that fall outside every polygon get None (surfaced in
        verification, not silently dropped).
        """
        df = df.copy()
        lon = pd.to_numeric(df[lon_col], errors="coerce").to_numpy()
        lat = pd.to_numeric(df[lat_col], errors="coerce").to_numpy()
        points = shapely.points(lon, lat)

        names = np.full(len(df), None, dtype=object)
        pcodes = np.full(len(df), None, dtype=object)

        # vectorised query: returns pairs [input_idx, tree_idx]
        input_idx, tree_idx = self.tree.query(points, predicate="intersects")

        # keep the first matching polygon per point (borders rarely double-match)
        seen = set()
        for i, t in zip(input_idx, tree_idx):
            if i in seen:
                continue
            seen.add(i)
            names[i] = self.names[t]
            pcodes[i] = self.pcodes[t]

        df["oblast_name"] = names
        df["oblast_pcode"] = pcodes
        return df
