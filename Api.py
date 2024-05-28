import pandas as pd
import numpy as np
import os
from flask import Flask, request, jsonify

app = Flask(__name__)
@app.route('/numadic/<int:start_time>/<int:end_time>')
def numadicapi(start_time, end_time):
    path = r'C:\Users\sriva\PycharmProjects\Numadic_Api\trip-Info.xlsx'
    df_trip = pd.read_excel(path)
    #to get the unique vehicle number from trip info file
    l = list(set(df_trip['vehicle_number']))
    #to get the vehicle_number.csv file and to check whether it is present in the folder or not
    file_names = ['C:\\Users\\sriva\\PycharmProjects\\Numadic_Api\\EOL-dump\\' + item + '.csv' for item in l]
    dfs = []
    print('ok first step')

    #to read all the csv files present in the zip folder
    for i in file_names:
        if os.path.exists(f'{i}'):
            df = pd.read_csv(f'{i}')
            dfs.append(df)
    print('ok secondstep')
    combined_df = pd.concat(dfs, ignore_index=True)

    #to filter the dataframe with start_time and end_time that we are passing as input
    filtered_df = combined_df[(combined_df['tis'] >= start_time) & (combined_df['tis'] <= end_time)]

    #I have joined both the dataframe the trip_info file and combination
    # of all csv to get all the desired columns
    merged_df = pd.merge(df_trip, filtered_df, left_on='vehicle_number', right_on='lic_plate_no', how='inner')
    print('ok thirdstep')

    unique_veh_number = list(set(merged_df['lic_plate_no']))
    df_final = pd.DataFrame(
        columns=['License_plate_number', 'Average_Speed', 'Transporter_name', 'Number_of_speed_violation','Number_of_Trips_Completed'])
    dfs1 = []
    #operations on the unique vehicle number to get the average_speed,distance,
    # license_plate_number,trip_completed,transporte_name
    for i in unique_veh_number:
        df2 = merged_df[merged_df['lic_plate_no'] == i]
        if not df2.empty:
            License_plate_number = df2['lic_plate_no'].iloc[0]
            Average_Speed = df2['spd'].mean()
            Transporter_name = df2['transporter_name'].iloc[0]
            Number_of_speed_violation = (df2['osf'] == True).sum()  # Count the number of True values
            Trip_counts = df2['vehicle_number'].value_counts()

            data_dict = {'License_plate_number': [License_plate_number],
                         'Average_Speed': [Average_Speed],
                         'Transporter_name': [Transporter_name],
                         'Number_of_speed_violation': [Number_of_speed_violation],
                         'Number_of_Trips_Completed':[Trip_counts]}

            df_fi = pd.DataFrame(data_dict)
            dfs1.append(df_fi)

    if len(dfs1) != 0:
        df_final = pd.concat(dfs1, ignore_index=True)
    else:
        return jsonify({'Error': 'No data in the giiven timestamp'})
    print('ok fourthstep')

    #calculating the distance using latitide and longitude using haversine formuala.
    #for some vehicle number the latitude and longitude is not changed so
    # resulting distance is 0
    vehicle_distances = []
    def haversine(lat1, lon1, lat2, lon2):
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371
        return c * r

    for j in unique_veh_number:
        # Filter DataFrame for the current vehicle number
        df_vehicle = merged_df[merged_df['vehicle_number'] == j]
        latitudes = df_vehicle['lat'].values
        longitudes = df_vehicle['lon'].values
        distances = haversine(latitudes[:-1], longitudes[:-1], latitudes[1:], longitudes[1:])
        total_distance = round(np.sum(distances), 2)
        vehicle_distances.append((j, total_distance))

    #df21 is the dataframe with vehicle numbeer and the distance covered by it
    df21 = pd.DataFrame(vehicle_distances, columns=['vehicle_number', 'distance'])

    #df21 is the dataframe with vehicle numbeer and the distance covered by it
    #merged with df_final to get the required columns
    merged_df1=pd.merge(df_final,df21, left_on='License_plate_number', right_on='vehicle_number', how='inner')
    merged_df1.drop(columns=['vehicle_number'], inplace=True)

    #writing dataframe to local path and returning the jsonify data format on calling the API
    merged_df1.to_csv(r'C:\Users\sriva\PycharmProjects\Numadic_Api\Final_file_latest.csv', index=False)
    json_data = merged_df1.to_json(orient='records')
    if not merged_df1.empty:
        return jsonify(json_data)
    else:
        return jsonify({'Error':'The current start_time and end_time has no data'})

if __name__ == '__main__':
    app.run(debug=True)