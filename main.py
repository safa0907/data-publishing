import os
import re
from operator import itemgetter
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection
from arcgis.mapping import WebMap
from arcgis.apps.dashboard import Dashboard
import geopandas as gpd
import json
# Script to be integrated with aws services (Fetch GeoJSON from S3)
import boto3
from io import BytesIO

client_config = {
    'Client A': {
        'email': 'clientA@email.com',
        'view_name': 'ClientA_Flood_Data_Europe_NorthAmerica',
        'filter': "location in ('Europe', 'North America')",
        'dashboard_title': 'Client A Flood Dashboard'
    },
    'Client B': {
        'email': 'clientB@email.com',
        'view_name': 'ClientB_Flood_Data_Global_2022_2024',
        'filter': "year >= 2022 AND year <= 2024",
        'dashboard_title': 'Client B Flood Dashboard'
    }
}

dashboard_temp = {
    "title": "",
    "layout": {
        "elements": []
    },
    "dataSources": []
}


def publish_or_update_feature_service(gis, geojson_path, service_name):
    # check if service exists
    search_services = gis.content.search(query=f"title:'{service_name}'", item_type="Feature Service",
                                         max_items=1)
    existing_service = [item for item in search_services if item.title == f'{service_name}']
    if existing_service:
        print(f"Feature service {service_name} exists. Updating...")
        feature_service_item = existing_service[0]
        # Retrieve feature service ID
        fs_id = feature_service_item.id
        sublayer_name = add_or_append_sublayers(gis, geojson_path, fs_id)
        # feature_service_item.update(data=geojson_path)
        # feature_service_item.publish(overwrite=True)
        print(f"Feature service {service_name} updated with sublayer {sublayer_name}.")
    else:
        print(f"Publishing new feature service {service_name}...")
        # Create empty feature service
        feature_service_item = create_empty_feature_service(gis, service_name)
        # Retrieve feature service ID
        feature_service_item_id = feature_service_item.id
        sublayer_name = add_or_append_sublayers(gis, geojson_path, feature_service_item_id)
        print(f"Feature service {service_name} published with sublayer {sublayer_name}.")
    return search_services[0]


def create_empty_feature_service(gis, service_name):
    """paramters for the create_empty_feature_service method"""
    name = service_name
    has_static_data = False  # want to be able to edit
    # max_record_count = 1000
    capabilities = "Query,Extract,Create,Update,Editing,Delete",
    service_type = "featureService"
    tags = ["Flood", "Feature", "Service"]
    snippet = "Flood data feature service"

    """create the empty feature service"""
    empty_service_item = gis.content.create_service(
        name=name,
        has_static_data=has_static_data,
        # max_record_count=max_record_count,
        capabilities=capabilities,
        service_type=service_type,
        tags=tags,
        snippet=snippet)
    return empty_service_item


def extract_year_from_filename(filepath):
    filename = os.path.basename(filepath)
    # get 4 digit year in filename
    match = re.search(r'(\d{4})', filename)
    if match:
        return match.group(1)
    else:
        return None


def add_or_append_sublayers(gis, geojson_path, service_id):
    year = extract_year_from_filename(geojson_path)
    sublayer_name = f"flood_data_{year}"
    fs = gis.content.get(f'{service_id}')
    # To read geojson file using Geopandas, we have to first download it into memory
    # Download Geojson file from S3
    file_content = geojson_path['Body'].read()
    # Read Geojson file into a GeoDataFrame
    geojson_bytes = BytesIO(file_content)
    gdf_geojson = gpd.read_file(geojson_bytes)
    # Add OBJECTID to geojson file
    if 'OBJECTID' not in gdf_geojson.columns:
        gdf_geojson['OBJECTID'] = range(1, len(gdf_geojson) + 1)
    flc = FeatureLayerCollection.fromitem(fs)
    new_layer_geojson = gdf_geojson.__geo_interface__
    update_definition = {
        "layers": [
            {
                "name": sublayer_name,
                "geometryType": "esriGeometryPolygon",
                "fields": [
                    {"name": "OBJECTID",
                     "type": "esriFieldTypeOID"
                     },
                    {
                        "name": "location",
                        "type": "esriFieldTypeString"
                    }  # Add other fields if any
                ],
                "features": new_layer_geojson['features']
            }
        ]
    }

    flc.manager.add_to_definition(update_definition)
    return sublayer_name


def create_view(feature_service, view_name, filter):
    print(f"Creating/ updating view {view_name} with filter: {filter}")
    try:
        existing_views = feature_service.layers[0].views
        view_item = None
        for view in existing_views:
            if view.properties.name == view_name:
                view_item = view
                break
        if view_item:
            print(f"View '{view_name}' exists.")
            view_item.manager.update_definition({'definitionExpression': filter})
        else:
            print(f"View {view_name} does not exist. Creating a new view...")
            view_item = feature_service.layers[0].create_view(view_name, definition_expression=filter)
            print(f"View {view_name} is created.")
        return view_item
    except Exception as e:
        print(f"Error creating view {view_name}:{e}")
        return None


def create_dashboards(gis, client, view_item, dashboard_title):
    print(f"Creating dashboard for {client}")
    try:
        web_map = WebMap()
        web_map.add_layer(view_item)
        web_map_properties = {
            'title': f"{dashboard_title} Map",
            'tags': 'flood, dashboard, ' + client,
            'type': 'Web Map'
        }
        web_map_item = gis.content.add(web_map_properties, data=web_map)
        print(f"WebMap {web_map_item.title} created.")

        app_prop = {
            'title': dashboard_title,
            'tags': 'flood, dashboard, ' + client,
            'type': 'Dashboard'
        }
        dashboard = gis.content.add(app_prop)

        dashboard.update(item_properties={'type': 'Dashboard'}, data={})
        print(f"Dashboard {dashboard_title} is created.")
        return dashboard
    except Exception as e:
        print(f"Error creating dashboard {dashboard_title}: {e}")
        return None


def main():
    global feature_service
    print("AGOL authentication...")
    gis = "gis-connection"
    s3 = boto3.client('s3')
    geojson_path = "s3://flood-data-bucket/"
    # List all files in S3 bucket
    list_files = s3.list_objects_v2(Bucket=geojson_path)
    for file in list_files.get('Contents', []):
        file_key = file['Key']
        if file_key.endswith('.geojson'):
            # Access geojson filtered
            # geojson_path = "s3://flood-data-bucket/flood_data_year.geojson"
            geojson_path = file_key
            # filename = "flood_data_year.geojson"
            # Publish feature service
            # feature_services = {}
            try:
                # year = extract_year_from_filename(geojson_path)
                service_name = f"Flood_Data"
                feature_service = publish_or_update_feature_service(gis, geojson_path, service_name)
                # feature_services[year] = feature_service
            except Exception as e:
                print(f"Error: {e}")
    flood_layer_collection = FeatureLayerCollection.fromitem(feature_service)
    sublayers = flood_layer_collection.layers
    # Create views
    for client, config in client_config.items():
        print(f"{client}...")
        if client == 'ClientA':  # Europe and North America all years
            view_item_a = flood_layer_collection.manager.create_view(name="view_testA", view_layers=[
                flood_layer_collection.layers[0:len(sublayers) + 1]])
            location_filter = "location in ('Europe','North America')"
            for i in range(len(sublayers) + 1):
                view_a_layer = view_item_a.layers[i]
                view_a_layer.manager.update_definition({"viewDefinitionQuery": location_filter})

        elif client == 'ClientB':  # Global only year 2022 to 2024
            list_index_sublayers = []
            for i, sublayer in enumerate(sublayers):
                sublayer_name = sublayer.properties.name
                if any(year in sublayer_name for year in ('2022', '2023', '2024')):
                    list_index_sublayers.append(i)

            view_item_b = flood_layer_collection.manager.create_view(name="view_testB",
                                                                     view_layers=[flood_layer_collection.layers[i] for i
                                                                                  in list_index_sublayers])
            # services = [fs for year, fs in feature_services.items() if 2022 <= int(year) <= 2024]

        else:
            services = []
        for fs in services:
            view_item = create_view(fs, config['view_name'], config['filter'])
            # Create dashboards
            if view_item:
                dashboard = create_dashboards(gis=gis, client=client, view_item=view_item,
                                              dashboard_title=config['dashboard_title'])
                if dashboard:
                    print(f"Dashboard for client {client}: {dashboard.url}")

if __name__ == "__main__":
    main()
