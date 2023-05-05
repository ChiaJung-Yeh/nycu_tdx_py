import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt
import requests
import json
from tqdm import tqdm
import warnings
from itertools import compress


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



def tdx_railway():
    railway=pd.DataFrame({'Operator':['臺鐵','高鐵','臺北捷運','高雄捷運','桃園捷運','新北捷運','臺中捷運','高雄輕軌','阿里山森林鐵路'],
                          'Code':['TRA','THSR','TRTC','KRTC','TYMC','NTDLRT','TMRT','KLRT','AFR']})
    return(railway)

    
    
def tdx_county():
    county=pd.DataFrame({'Operator':['臺北市','新北市','桃園市','臺中市','臺南市','高雄市','基隆市','新竹市','新竹縣','苗栗縣','彰化縣','南投縣','雲林縣','嘉義縣','嘉義市','屏東縣','宜蘭縣','花蓮縣','臺東縣','金門縣','澎湖縣','連江縣','公路客運'],
                         'Code':['Taipei','NewTaipei',"Taoyuan","Taichung","Tainan","Kaohsiung","Keelung","Hsinchu","HsinchuCounty","MiaoliCounty","ChanghuaCounty","NantouCounty","YunlinCounty","ChiayiCounty","Chiayi","PingtungCounty","YilanCounty","HualienCounty","TaitungCounty","KinmenCounty","PenghuCounty","LienchiangCounty","Intercity"]})
    return(county)



def Bus_Route(access_token, county, out=False):
    if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
        return(warnings.warn("Export file must contain '.csv' or '.txt'!", UserWarning))
    
    if county=="Intercity":
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/Route/InterCity?%24format=JSON"
    elif county in list(tdx_county().Code):
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/Route/City/"+county+"?&%24format=JSON"
    else:
        print(tdx_county())
        return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning))
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
    
    if out!=False:
        bus_route.to_csv(out, index=False)
    return(bus_route)



def Bus_Shape(access_token, county, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
    
    if county=="Intercity":
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/Shape/InterCity?&$format=JSON"
    elif county in list(tdx_county().Code):
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/Shape/City/"+county+"?&%24format=JSON"
    else:
        print(tdx_county())
        return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!"), UserWarning)
    js_data=json.loads(data_response.text)
    bus_shape=pd.DataFrame.from_dict(js_data, orient="columns")
    
    bus_shape.RouteName=[bus_shape.RouteName[i]['Zh_tw'] if len(bus_shape.RouteName[i])!=0  else None for i in range(len(bus_shape))]
    bus_shape.SubRouteName=[bus_shape.SubRouteName[i]['Zh_tw'] if len(bus_shape.SubRouteName[i])!=0  else None for i in range(len(bus_shape))]
    bus_shape=bus_shape.loc[:,['RouteUID','RouteID','RouteName','SubRouteUID','SubRouteID','SubRouteName','Geometry']].rename(columns={'Geometry':'geometry'})
    
    if dtype=="text":
        if out!=False:
            bus_shape.to_csv(out, index=False)
        return(bus_shape)
    elif dtype=="sf":
        bus_shape['geometry']=bus_shape['geometry'].apply(wkt.loads)
        bus_shape=gpd.GeoDataFrame(bus_shape, crs='epsg:4326')
        if out!=False:
            bus_shape.to_file(out, index=False)
        return(bus_shape)



def Bus_StopOfRoute(access_token, county, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
    
    if county=="Intercity":
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/StopOfRoute/InterCity?&$format=JSON"
    elif county in list(tdx_county().Code):
        url="https://tdx.transportdata.tw/api/basic/v2/Bus/StopOfRoute/City/"+county+"?&%24format=JSON"
    else:
        print(tdx_county())
        return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning))
    js_data=json.loads(data_response.text)
    bus_stopofroute=pd.DataFrame.from_dict(js_data, orient="columns")
    
    bus_stopofroute.RouteName=[bus_stopofroute.RouteName[i]['Zh_tw'] if len(bus_stopofroute.RouteName[i])!=0  else None for i in range(len(bus_stopofroute))]
    bus_stopofroute.SubRouteName=[bus_stopofroute.SubRouteName[i]['Zh_tw'] if len(bus_stopofroute.SubRouteName[i])!=0  else None for i in range(len(bus_stopofroute))]

    bus_info=bus_stopofroute.loc[:,['RouteUID','RouteID','RouteName','SubRouteUID','SubRouteName','SubRouteID','Direction']]
    stopnum=[len(bus_stopofroute.Stops[i]) for i in range(len(bus_stopofroute))]
    
    bus_stop=dict()
    label_all=['StopUID','StopID','StopName','StopBoarding','StopSequence','StationID','StopPosition']
    for label_id in label_all:
        if label_id in ['StopName']:
            bus_stop[label_id]=[bus_stopofroute.Stops[i][j][label_id]['Zh_tw'] if label_id in bus_stopofroute.Stops[i][j] else None for i in range(len(bus_stopofroute)) for j in range(len(bus_stopofroute.Stops[i]))]
        else:
            bus_stop[label_id]=[bus_stopofroute.Stops[i][j][label_id] if label_id in bus_stopofroute.Stops[i][j] else None for i in range(len(bus_stopofroute)) for j in range(len(bus_stopofroute.Stops[i]))]
    bus_stop=pd.DataFrame(bus_stop)
    bus_stop=pd.concat([bus_stop, pd.DataFrame(list(bus_stop.StopPosition)).loc[:,['PositionLon','PositionLat']]], axis=1)
    
    bus_info=bus_info.iloc[np.repeat(np.arange(len(bus_info)), stopnum)].reset_index(drop=True)
    bus_stop=bus_stop.loc[:,['StopUID','StopID','StopName','StationID','StopBoarding','StopSequence','PositionLon','PositionLat']]
    bus_stopofroute=pd.concat([bus_info, bus_stop], axis=1)
    
    if dtype=="text":
        if out!=False:
            bus_stopofroute.to_csv(out, index=False)
        return(bus_stopofroute)
    elif dtype=="sf":
        bus_stopofroute['geometry']=gpd.points_from_xy(bus_stopofroute.PositionLon, bus_stopofroute.PositionLat, crs="EPSG:4326")
        bus_stopofroute=gpd.GeoDataFrame(bus_stopofroute, crs='epsg:4326')
        if out!=False:
            bus_stopofroute.to_file(out, index=False)
        return(bus_stopofroute)



def Rail_Shape(access_token, operator, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
        
    if operator=='TRA':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/TRA/Shape?&%24format=JSON"
    elif operator=='THSR':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/THSR/Shape?%24format=JSON"
    elif operator in ["TRTC", "KRTC", "TYMC", "NTDLRT",  "TMRT", "KLRT"]:
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Shape/"+operator+"?&%24format=JSON"
    elif operator=='AFR':
        return(warnings.warn('AFR does not provide route geometry data up to now! Please check out other rail system.', UserWarning))
    else:
        print(tdx_railway())
        return(warnings.warn("'"+operator+"' is not valid operator. Please check out the table of railway code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!"), UserWarning)
    js_data=json.loads(data_response.text)
    rail_shape=pd.DataFrame.from_dict(js_data, orient="columns")
    
    rail_shape.LineName=[rail_shape.LineName[i]['Zh_tw'] if len(rail_shape.LineName[i])!=0  else None for i in range(len(rail_shape))]
    rail_shape=rail_shape.loc[:,['LineID','LineName','Geometry']].rename(columns={'Geometry':'geometry'})

    if dtype=="text":
        if out!=False:
            rail_shape.to_csv(out, index=False)
        return(rail_shape)
    elif dtype=="sf":
        rail_shape['geometry']=rail_shape['geometry'].apply(wkt.loads)
        rail_shape=gpd.GeoDataFrame(rail_shape, crs='epsg:4326')
        if out!=False:
            rail_shape.to_file(out, index=False)
        return(rail_shape)


    
def Bus_TravelTime(access_token, county, routeid, out=False):
    if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
        return(warnings.warn("Export file must contain '.csv' or '.txt'!", UserWarning))
    
    bus_traveltime=pd.DataFrame()
    for busrouteid in tqdm(routeid):
        if county=="Intercity":
             url="https://tdx.transportdata.tw/api/basic/v2/Bus/S2STravelTime/InterCity/"+busrouteid+"?&%24format=JSON"
        elif county in list(tdx_county().Code):
             url="https://tdx.transportdata.tw/api/basic/v2/Bus/S2STravelTime/City/"+county+"/"+busrouteid+"?&%24format=JSON"
        else:
            print(tdx_county())
            return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))
        
        try:
            data_response=requests.get(url, headers=access_token)
        except:
            return(warnings.warn("Your access token is invalid!", UserWarning))
        js_data=json.loads(data_response.text)

        subroute_info=dict()
        label_all=['RouteUID','RouteID','SubRouteUID','SubRouteID','Direction']
        for label_id in label_all:
            subroute_info[label_id]=[js_data[i][label_id] if label_id in js_data[i] else None for i in range(len(js_data))]
        subroute_info=pd.DataFrame(subroute_info)

        num_of_week=[len(js_data[i]["TravelTimes"]) for i in range(len(js_data))]
        subroute_info=subroute_info.iloc[np.repeat(np.arange(len(subroute_info)), num_of_week)].reset_index(drop=True)

        week_info=dict()
        label_all=['Weekday','StartHour','EndHour']
        for label_id in label_all:
            week_info[label_id]=[js_data[i]["TravelTimes"][j][label_id] if label_id in js_data[i]["TravelTimes"][j] else None for i in range(len(js_data)) for j in range(len(js_data[i]["TravelTimes"]))]
        week_info=pd.DataFrame(week_info)
        subroute_info=pd.concat([subroute_info, week_info], axis=1).reset_index(drop=True)

        num_of_od=[len(js_data[i]["TravelTimes"][j]['S2STimes']) for i in range(len(js_data)) for j in range(len(js_data[i]["TravelTimes"]))]    
        subroute_info=subroute_info.iloc[np.repeat(np.arange(len(subroute_info)), num_of_od)].reset_index(drop=True)

        traveltime_info=dict()
        label_all=['FromStopID','ToStopID','FromStationID','ToStationID','RunTime']
        for label_id in label_all:
            traveltime_info[label_id]=[js_data[i]["TravelTimes"][j]['S2STimes'][k][label_id] if label_id in js_data[i]["TravelTimes"][j]['S2STimes'][k] else None for i in range(len(js_data)) for j in range(len(js_data[i]["TravelTimes"])) for k in range(len(js_data[i]["TravelTimes"][j]['S2STimes']))]
        traveltime_info=pd.DataFrame(traveltime_info)
        subroute_info=pd.concat([subroute_info, traveltime_info], axis=1).reset_index(drop=True)

        bus_traveltime=pd.concat([bus_traveltime, subroute_info]).reset_index(drop=True)
    
    if out!=False:
        bus_traveltime.to_csv(out, index=False)
    return(bus_traveltime)



def Rail_Station(access_token, operator, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
        
    if operator=='TRA':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/TRA/Station?&%24format=JSON"
    elif operator=='THSR':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/THSR/Station?&%24format=JSON"
    elif operator in ["TRTC", "KRTC", "TYMC", "NTDLRT",  "TMRT", "KLRT"]:
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/"+operator+"?&%24format=JSON"
    elif operator=='AFR':
        url="https://tdx.transportdata.tw/api/basic/v3/Rail/AFR/Station?&%24format=JSON"
    else:
        print(tdx_railway())
        return(warnings.warn("'"+operator+"' is not valid operator. Please check out the table of railway code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning))
    js_data=json.loads(data_response.text)
    
    if operator=='TRA':
        rail_station=pd.DataFrame.from_dict(js_data, orient="columns")
        rail_station.StationName=[rail_station.StationName[i]["Zh_tw"] for i in range(len(rail_station))]
        rail_station=pd.concat([rail_station, pd.DataFrame(list(rail_station.StationPosition)).loc[:,["PositionLon","PositionLat"]]], axis=1)
        rail_station=rail_station.loc[:,['StationUID','StationID','StationName','StationAddress','StationPhone','LocationCity','LocationTown','PositionLon','PositionLat','StationClass']]
    elif operator=="AFR":
        rail_station=pd.DataFrame.from_dict(js_data, orient="columns")
        rail_station=pd.DataFrame(list(rail_station.Stations))
        rail_station.StationName=[rail_station.StationName[i]["Zh_tw"] for i in range(len(rail_station))]
        rail_station=pd.concat([rail_station, pd.DataFrame(list(rail_station.StationPosition)).loc[:,["PositionLon","PositionLat"]]], axis=1)
        rail_station=rail_station.loc[:,['StationUID','StationID','StationName','StationAddress','StationPhone','PositionLon','PositionLat','StationClass']]
    else:
        rail_station=pd.DataFrame.from_dict(js_data, orient="columns")
        rail_station.StationName=[rail_station.StationName[i]["Zh_tw"] for i in range(len(rail_station))]
        rail_station=pd.concat([rail_station, pd.DataFrame(list(rail_station.StationPosition)).loc[:,["PositionLon","PositionLat"]]], axis=1)
        rail_station=rail_station.loc[:,['StationUID','StationID','StationName','StationAddress','PositionLon','PositionLat']]

    if dtype=="text":
        if out!=False:
            rail_station.to_csv(out, index=False)
        return(rail_station)
    elif dtype=="sf":
        rail_station['geometry']=gpd.points_from_xy(rail_station.PositionLon, rail_station.PositionLat, crs="EPSG:4326")
        rail_station=gpd.GeoDataFrame(rail_station, crs='epsg:4326')
        if out!=False:
            rail_station.to_file(out, index=False)
        return(rail_station)



def Rail_StationOfLine(access_token, operator, out=False):
    if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
        return(warnings.warn("Export file must contain '.csv' or '.txt'!", UserWarning))

    if operator=='TRA':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/TRA/StationOfLine?&%24format=JSON"
    elif operator=='THSR':
        return(warnings.warn("Please use function 'Rail_Station' to retrieve the station of high speed rail (THSR).", UserWarning))
    elif operator in ["TRTC", "KRTC", "TYMC", "NTDLRT",  "TMRT", "KLRT"]:
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/StationOfLine/"+operator+"?&%24format=JSON"
    elif operator=='AFR':
        url="https://tdx.transportdata.tw/api/basic/v3/Rail/AFR/StationOfLine?&%24format=JSON"
    else:
        print(tdx_railway())
        return(warnings.warn("'"+operator+"' is not valid operator. Please check out the table of railway code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning)) 
    js_data=json.loads(data_response.text)
    
    if operator in ['AFR']:
        js_data=js_data["StationOfLines"]
    
    rail_line=[js_data[i]["LineID"] for i in range(len(js_data))]
    rail_line=pd.DataFrame({'LineID':rail_line})
    num_of_station=[len(js_data[i]["Stations"]) for i in range(len(js_data))]
    rail_line=rail_line.iloc[np.repeat(np.arange(len(rail_line)), num_of_station)].reset_index(drop=True)

    rail_station_temp=dict()
    label_all=['Sequence','StationID','StationName','TraveledDistance','CumulativeDistance']
    for label_id in label_all:
        if label_id in ['StationName'] and operator not in ['TRA']:
            rail_station_temp[label_id]=[js_data[i]["Stations"][j][label_id]['Zh_tw'] if label_id in js_data[i]["Stations"][j] else None for i in range(len(js_data)) for j in range(len(js_data[i]["Stations"]))]
        else:
            rail_station_temp[label_id]=[js_data[i]["Stations"][j][label_id] if label_id in js_data[i]["Stations"][j] else None for i in range(len(js_data)) for j in range(len(js_data[i]["Stations"]))]
    rail_station_temp=pd.DataFrame(rail_station_temp)
    rail_station_line=pd.concat([rail_line, rail_station_temp], axis=1)

    for label_id in ['TraveledDistance','CumulativeDistance']:
        if sum(rail_station_line[label_id].isna())==len(rail_station_line):
            rail_station_line=rail_station_line.drop([label_id], axis=1)
            
    if operator=='TRA':
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/TRA/Line?&%24format=JSON"
    elif operator=='AFR':
        url="https://tdx.transportdata.tw/api/basic/v3/Rail/AFR/Line?&%24format=JSON"
    else: 
        url="https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Line/"+operator+"?&%24format=JSON"
    data_response=requests.get(url, headers=access_token)
    js_data=json.loads(data_response.text)
    
    if operator=="AFR":
        js_data=js_data["Lines"]

    rail_line=dict()
    label_all=['LineID','LineName','LineSectionName']
    for label_id in label_all:
        if label_id in ['LineName','LineSectionName']:
            if operator=="TRA":
                rail_line[label_id]=[js_data[i][label_id+'Zh'] if label_id+'Zh' in js_data[i] else None for i in range(len(js_data))]
            else:
                rail_line[label_id]=[js_data[i][label_id]['Zh_tw'] if label_id in js_data[i] and len(js_data[i][label_id])!=0 else None for i in range(len(js_data))]
        else:
            rail_line[label_id]=[js_data[i][label_id] if label_id in js_data[i] else None for i in range(len(js_data))]
    rail_line=pd.DataFrame(rail_line)
    for label_id in ['LineName','LineSectionName']:
        if sum(rail_line[label_id].isna())==len(rail_line):
            rail_line=rail_line.drop([label_id], axis=1)
            
    rail_station_line=pd.merge(rail_station_line, rail_line, on="LineID", how="left").reset_index(drop=True)

    datacol=['LineID','LineName','LineSectionName','Sequence','StationID','StationName','TraveledDistance','CumulativeDistance']
    tt=[i if datacol[i] in list(rail_station_line.columns) else None for i in range(len(datacol))]
    tt=[i for i in tt if i is not None]
    datacol=[datacol[i] for i in tt]
    rail_station_line=rail_station_line.loc[:, datacol]
    return(rail_station_line)
    
    
    
def Bike_Shape(access_token, county, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
    
    if county in list(tdx_county().Code):
        url="https://tdx.transportdata.tw/api/basic/v2/Cycling/Shape/City/"+county+"?&%24format=JSON"
    else:
        print(tdx_county())
        return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))

    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning)) 
    js_data=json.loads(data_response.text)
    
    if "Message" in js_data:
        return(warnings.warn("'"+county+"' does not provide cycling network in TDX platform up to now.", UserWarning))

    bike_shape=pd.DataFrame.from_dict(js_data, orient="columns")
    bike_shape=bike_shape.loc[:,['RouteName','City','RoadSectionStart','RoadSectionEnd','CyclingLength','Direction','Geometry']].rename(columns={'Geometry':'geometry'})
    
    # revise the invalid geometry record
    for i in range(len(bike_shape)):
        temp=bike_shape.loc[i, 'geometry']
        temp=temp.replace("MULTILINESTRING ((", "")
        temp=temp.replace("))", "")
        temp=temp.split("),(")
        temp_count=[temp[i].count(" ") for i in range(len(temp))]
        fil=[temp_count[i]!=1 for i in range(len(temp_count))]
        if sum([not elem for elem in fil])!=0:        
            temp=list(compress(temp, fil))
            temp="MULTILINESTRING (("+("),(".join(temp))+"))"
            bike_shape.loc[i, 'geometry']=temp
    
    if dtype=="text":
        if out!=False:
            rail_shape.to_csv(out, index=False)
        return(bike_shape)
    elif dtype=="sf":
        bike_shape['geometry']=bike_shape['geometry'].apply(wkt.loads)
        bike_shape=gpd.GeoDataFrame(bike_shape, crs='epsg:4326')
        if out!=False:
            bike_shape.to_file(out, index=False)
        return(bike_shape)
    
    
    
def Bike_Station(access_token, county, dtype="text", out=False):
    if dtype=="text":
        if out!=False and ~pd.Series(out).str.contains('\.csv|\.txt')[0]:
            return(warnings.warn("Export file of 'text' must contain '.csv' or '.txt'!", UserWarning))
    elif dtype=="sf":
        if out!=False and ~pd.Series(out).str.contains('shp')[0]:
            return(warnings.warn("Export file of 'sf' must contain '.shp'!", UserWarning))
    else:
        return(warnings.warn("'dtype' must be 'text' or 'sf'!", UserWarning))
        
    if county in list(tdx_county().Code):
        url="https://tdx.transportdata.tw/api/basic/v2/Bike/Station/City/"+county+"?&%24format=JSON"
    else:
        print(tdx_county())
        return(warnings.warn("'"+county+"' is not valid county. Please check out the table of county code above.", UserWarning))
    
    try:
        data_response=requests.get(url, headers=access_token)
    except:
        return(warnings.warn("Your access token is invalid!", UserWarning))
    js_data=json.loads(data_response.text)
    
    if "Message" in js_data:
        return(warnings.warn("'"+county+"' does not provide bike sharing system.", UserWarning))

    bike_station=pd.DataFrame.from_dict(js_data, orient="columns")
    bike_station.StationName=[bike_station.StationName[i]["Zh_tw"] for i in range(len(bike_station))]
    bike_station.StationAddress=[bike_station.StationAddress[i]["Zh_tw"] if len(bike_station.StationAddress[0])!=0 else None for i in range(len(bike_station))]
    bike_station=pd.concat([bike_station, pd.DataFrame(list(bike_station.StationPosition)).loc[:,["PositionLon","PositionLat"]]], axis=1)
    bike_station=bike_station.loc[:,['StationUID','StationID','StationName','StationAddress','PositionLon','PositionLat','BikesCapacity','ServiceType']]

    if dtype=="text":
        if out!=False:
            bike_station.to_csv(out, index=False)
        return(bike_station)
    elif dtype=="sf":
        bike_station['geometry']=gpd.points_from_xy(bike_station.PositionLon, bike_station.PositionLat, crs="EPSG:4326")
        bike_station=gpd.GeoDataFrame(bike_station, crs='epsg:4326')
        if out!=False:
            bike_station.to_file(out, index=False)
        return(bike_station)
        