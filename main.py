def main():
    import datetime
    import json
    import os
    import boto3
    import h3 as hexa
    import plotly.express as px
    import xarray as xr
    from geojson import Feature, FeatureCollection, Polygon
    from shapely.geometry import Polygon

    # script run time date measurements
    start_time = datetime.datetime.now()
    # settings
    h3Resolution = 4
    file_path = '/Users/semihdalgin/Desktop/coding/coding/Jua/data.nc'
    output = "/Users/semihdalgin/Desktop/coding/coding/Jua/"
    today = datetime.datetime.now().isoformat()
    dates=[]

    #checking data and downloading. If it is exist, using existing one.
    if not os.path.exists(file_path) == True:
        '''' ** ** Download data ** ** '''
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('era5-pds')
        bucket.download_file(Key='2022/05/data/precipitation_amount_1hour_Accumulation.nc', Filename=file_path)
    else:
        print('Using Existing File')

    #h3 indexing
    def h3index(df):
        df["h3"] = df.apply(lambda x: hexa.geo_to_h3(lat=x.lat, lng=x.lon, resolution=h3Resolution), axis=1)
        return df

    #generating h3 geometries
    def add_geometry(df):
        points = hexa.h3_to_geo_boundary(df['h3'], True)
        return Polygon(points)

    #generating geojson
    def hexagons_dataframe_to_geojson(df_hex, hex_id_field, geometry_field, value_field, file_output= None):
        list_features = []

        for i, row in df_hex.iterrows():
            feature = Feature(geometry=row[geometry_field],
                              id=row[hex_id_field],
                              properties={"value": row[value_field]})
            list_features.append(feature)

        feat_collection = FeatureCollection(list_features)

        if file_output is not None:
            with open(file_output, "w") as f:
                json.dump(feat_collection, f)

        else:
            return feat_collection

    #data operations, date list of data
    def unique(list1):
        # initialize a null list
        unique_list = []
        # traverse for all elements
        for x in list1:
            # check if exists in unique_list or not
            if x not in unique_list:
                unique_list.append(x)
        return unique_list

    # read the netcdf data into xarray dataset
    ds_main = xr.open_dataset(file_path)

    for i in ds_main['time1'].values:
        ii=str(i).split('T')[0]
        year, month, day = [int(item) for item in (ii.split('-'))]
        iii = datetime.date(year, month, day)
        dates.append(str(iii))
    dates=unique(dates)
    while True:
        selection_date = input('Please specify a day for filtering as YYYY-MM-DD :')
        if selection_date in dates:
            break
        else:
            print('There is not any information for this day. Please select another day')

    year, month, day = [int(item) for item in selection_date.split('-')]
    start_date = datetime.date(year, month, day)
    end_date = start_date + datetime.timedelta(hours=1)
    #Filtering data according to date range
    ds = ds_main.sel(time1=slice(start_date, end_date))

    df = ds.to_dataframe()
    df = df.reset_index(level=0)
    df = df.reset_index(level=0)
    df = df.reset_index(level=0)
    df = df.reset_index(level=0)
    df = df.reset_index(level=0)

    #indexing
    h3index(df)

    #H3 data preperations
    prep = (df.groupby('h3').index.agg(list).to_frame('ids').reset_index())
    # Means each points inside the hexagon
    prep['prep_mean'] = df.groupby('h3')['precipitation_amount_1hour_Accumulation'].transform('mean')

    # Generating geometries into our dataframe
    prep['geometry'] = (prep.apply(add_geometry, axis=1))

    #Data export in parquet format
    filename = 'Timestamp_filter_' + str(start_date) + '_' + str(end_date) + '.parquet'
    df.to_parquet(os.path.join(output, filename), engine='pyarrow')

    #Generating Polygons
    geojson_obj = (hexagons_dataframe_to_geojson
                   (prep,
                    hex_id_field='h3',
                    value_field='prep_mean',
                    geometry_field='geometry'))
    #Visualisations
    fig = (px.choropleth_mapbox(
        prep,
        geojson=geojson_obj,
        locations='h3',
        color='prep_mean',
        color_continuous_scale="Viridis",
        range_color=(0, prep['prep_mean'].mean()), mapbox_style='carto-positron',
        zoom=3,
        center={"lat": 20, "lon": 20},
        opacity=0.1,
        labels={'1hour_Accumulation': '# of precipitation_amount'}))
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()

    end_time = datetime.datetime.now()
    print(f"Total duration:{end_time - start_time}")
