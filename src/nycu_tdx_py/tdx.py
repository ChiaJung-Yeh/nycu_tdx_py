import pandas as pd
from pandas import json_normalize
import numpy as np
import geopandas as gpd
from shapely import wkt
from shapely.ops import nearest_points, split, snap
from shapely.geometry import Point, LineString, MultiPoint
import folium
import requests
import json


def get_token(app_id, app_key):
    auth_url="https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
    class Auth():
        def __init__(self, app_id, app_key):
            self.app_id = app_id
            self.app_key = app_key

        def get_auth_header(self):
            content_type = 'application/x-www-form-urlencoded'
            grant_type = 'client_credentials'

            return{
                'content-type' : content_type,
                'grant_type' : grant_type,
                'client_id' : self.app_id,
                'client_secret' : self.app_key
            }

    class data():
        def __init__(self, app_id, app_key, auth_response):
            self.app_id = app_id
            self.app_key = app_key
            self.auth_response = auth_response

        def get_data_header(self):
            auth_JSON = json.loads(self.auth_response.text)
            access_token = auth_JSON.get('access_token')

            return{'authorization': 'Bearer '+access_token}
    auth_response=requests.post(auth_url, Auth(app_id, app_key).get_auth_header())
    access_token=data(app_id, app_key, auth_response).get_data_header()
    return(access_token)



def Bus_Route(access_token, county):
    url="https://tdx.transportdata.tw/api/basic/v2/Bus/Route/City/"+county+"?&%24format=JSON"
    data_response=requests.get(url, headers=access_token)
    js_data=json.loads(data_response.text)
    bus_route=pd.DataFrame.from_dict(js_data, orient="columns")
    
    bus_info=bus_route.loc[:,['RouteUID','RouteID','RouteName','BusRouteType','DepartureStopNameZh','DestinationStopNameZh']]
    subroutenum=[len(bus_route.SubRoutes[i]) for i in range(len(bus_route))]
    
    bus_subroute=dict()
    label_all=['SubRouteUID','SubRouteID','SubRouteName','Direction']
    for label_id in label_all:
        if label_id in ['SubRouteName']:
            bus_subroute[label_id]=[bus_route.SubRoutes[i][j][label_id]['Zh_tw'] if label_id in bus_route.SubRoutes[i][j] else None for i in range(len(bus_route)) for j in range(subroutenum[i])]
        else:
            bus_subroute[label_id]=[bus_route.SubRoutes[i][j][label_id] if label_id in bus_route.SubRoutes[i][j] else None for i in range(len(bus_route)) for j in range(subroutenum[i])]

    bus_info=bus_info.iloc[np.repeat(np.arange(len(bus_info)), subroutenum)].reset_index(drop=True)
    bus_route=pd.concat([bus_info, pd.DataFrame(bus_subroute)], axis=1)
    bus_route.RouteName=[bus_route.RouteName[i]['Zh_tw'] for i in range(len(bus_route))]
    return(bus_route)



def Bus_Shape(access_token, county):
    url="https://tdx.transportdata.tw/api/basic/v2/Bus/Shape/City/"+county+"?&%24format=JSON"
    data_response=requests.get(url, headers=access_token)
    js_data=json.loads(data_response.text)
    bus_shape=pd.DataFrame.from_dict(js_data, orient="columns")
    
    bus_shape.RouteName=[bus_shape.RouteName[i]['Zh_tw'] if len(bus_shape.RouteName[i])!=0  else None for i in range(len(bus_shape))]
    bus_shape.SubRouteName=[bus_shape.SubRouteName[i]['Zh_tw'] if len(bus_shape.SubRouteName[i])!=0  else None for i in range(len(bus_shape))]
    bus_shape=bus_shape.loc[:,['RouteUID','RouteID','RouteName','SubRouteUID','SubRouteID','SubRouteName','Geometry']].rename(columns={'Geometry':'geometry'})
    bus_shape['geometry']=bus_shape['geometry'].apply(wkt.loads)
    bus_shape=gpd.GeoDataFrame(bus_shape, crs='epsg:4326')
    return(bus_shape)
    