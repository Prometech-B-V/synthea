import json

with open("export-fu;;.geojson") as f:
    data = json.load(f)
    result = dict()

    for feature in data['features']:
        del feature['geometry']

    json.dump(data, open("./export-fixed.geojson", "w"))
