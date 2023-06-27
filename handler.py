import psycopg2
import json
from datetime import datetime

def lambda_handler(event, context):
   
    print("hello")
    field = event['field']
    
    if field == 'catchment_api':
        catchment = event['arguments']['name']
        return catchmentApi(catchment)
        
    elif field == 'catchment':
        catchment = event['arguments']['name']
        return catchmentA(catchment)
        
    elif field == 'wqIndcatorFact_api':
        catchment_ids = event['arguments']['catchment_id']
        indicator_ids = event['arguments']['indicator_id']
        return wqIndcatorFactApi(catchment_ids,indicator_ids)
        
    elif field == 'wqIndcatorFactId_api':
        catchment_id = event['arguments']['catchment_id']
        return wqIndcatorFactIdApi(catchment_id)
        
    elif field == 'wqIndcatorDateRangeApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return wqIndcatorDateRangeApi(startDate,endDate,catchment_id)
        
    elif field == 'waterQualityLookup':
        regulatory = event['arguments']['regulatory']
        water_quality_indicator = event['arguments']['water_quality_indicator']
        catchment_id = event['arguments']['catchment_id']
        return waterQualityLookup(regulatory, water_quality_indicator,catchment_id)
        
    elif field == 'satelliteApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return satelliteApi(startDate,endDate,catchment_id)
        
    elif field == 'liveSensorDataApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return liveSensorDataApi(startDate,endDate,catchment_id)
        
    elif field == 'userAccessLevelApi':
        user_id = event['arguments']['user_id']
        return userAccessLevelApi(user_id)
        
    elif field == 'meteoFactApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return meteoFactApi(startDate,endDate,catchment_id)
        
    elif field == 'subcatchmentApi':
        subcatchment_name = event['arguments']['subcatchment_name']
        catchment_id = event['arguments']['catchment_id']
        return subcatchmentApi(subcatchment_name, catchment_id)
        
    elif field == 'incapStatApi':
        if(event['arguments']['date']):
            date = event['arguments']['date']
        else:
            date = ""
            
        if(event['arguments']['aggregate']):
            aggregate = event['arguments']['aggregate']
        else:
            aggregate = ""
            
        if(event['arguments']['catchment_id']):
            catchment_id = event['arguments']['catchment_id']
        else:
            catchment_id = ""
            
        return incapStatApi(date, aggregate, catchment_id)
        
    elif field == 'incanStatApi':
        if(event['arguments']['date']):
            date = event['arguments']['date']
        else:
            date = ""
            
        if(event['arguments']['aggregate']):
            aggregate = event['arguments']['aggregate']
        else:
            aggregate = ""
            
        if(event['arguments']['catchment_id']):
            catchment_id = event['arguments']['catchment_id']
        else:
            catchment_id = ""
            
        return incanStatApi(date, aggregate, catchment_id)
        
    elif field == 'incaDsdApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return incaDsdApi(startDate, endDate, catchment_id)
        
    elif field == 'incaOutputDsdFact':
        reaches = event['arguments']['reaches']['reaches']
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return incaOutputDsdFact(reaches, startDate, endDate, catchment_id)
        
    elif field == 'incapDsdApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return incapDsdApi(startDate, endDate, catchment_id)
        
    elif field == 'incanDsdApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return incanDsdApi(startDate, endDate, catchment_id)
        
    elif field == 'incapTcApi':
        startDate = event['arguments']['startDate']
        endDate = event['arguments']['endDate']
        catchment_id = event['arguments']['catchment_id']
        return incapTcApi(startDate, endDate, catchment_id)
        
    else:
        return {
            'statusCode': 500,
            'api' : "trying to access wrong api",
            'body': json.dumps({'error': 'Internal server error  -  database connection error'}),
        }
    
    
def connect():
    try: 
        host = 'aqs-dev.cvim08hvuskh.eu-west-1.rds.amazonaws.com'
        port = '5432'
        database = 'AquaScope_'
        username = 'Aquascope'
        password = 'aqs_rds2kjr-insdev8dkt'
        
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=username, password=password
        )
        cur = conn.cursor()
        return cur, conn
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error  -  database connection error'})
        }
        
def catchmentApi(catchment):
    try:
        cur, conn = connect()
        
        cur.execute('SELECT * FROM "AquaScope_MVP"."catchment_geojson_data" WHERE LOWER(catchment) = LOWER(%s)', (catchment,))
        data = cur.fetchall()
        if not data:
            res_catchment = "Data Not Found"
            res_wq_indcator_fact = "Data Not Found"
            res_points = "Data Not Found"
        else:
            res_catchment = []
            for d in data:
                row_data = {
                    'c_id': d[0],
                    'catchment': d[1],
                    'geom': json.loads(d[7]),
                }
                res_catchment.append(row_data)
        
        cur.execute('SELECT * FROM "AquaScope_MVP".wq_indicator_fact_view WHERE c_name = %s', (catchment,))
        data = cur.fetchall()
        if not data:
            res_wq_indcator_fact = "Data Not Found"
        else:
            res_wq_indcator_fact = []
            for d in data:
                row_data = {
                    'id': d[0],
                    'date_field': d[1].strftime("%Y-%m-%d"),
                    'reach': d[2],
                    'value': d[3],
                    'datetimeid': d[4],
                    'c_name': d[5],
                    'indicator_id': d[6],
                    'points_id': d[7]
                }
                res_wq_indcator_fact.append(row_data)
        
        cur.execute('SELECT p_id, geom FROM "AquaScope_MVP".points_data_view WHERE catchment = %s ORDER BY id ASC ', (catchment,))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[0],
                    'geom': json.loads(d[1]),
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'catchment': str(res_catchment),
            'wq_indcator_fact': str(res_wq_indcator_fact),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def catchmentA(catchment):
    try:
        cur, conn = connect()
        
        cur.execute('SELECT * FROM "AquaScope_MVP"."catchment_geojson_data" WHERE LOWER(catchment) = LOWER(%s)', (catchment,))
        data = cur.fetchall()
        if not data:
            res_catchment = "Data Not Found"
            res_subcatchment = "Data Not Found"
            res_points = "Data Not Found"
        else:
            res_catchment = []
            for d in data:
                row_data = {
                    'c_id': d[0],
                    'catchment': d[1],
                    'geom': json.loads(d[7]),
                }
                res_catchment.append(row_data)
        
        cur.execute('SELECT * FROM "AquaScope_MVP".view_sub_catchment WHERE LOWER(catchment) = LOWER(%s)', (catchment,))
        data = cur.fetchall()
        if not data:
            res_subcatchment = "Data Not Found"
        else:
            res_subcatchment = []
            for d in data:
                row_data = {
                    'subcatch': d[4],
                    'catchment': d[10],
                    'geom': json.loads(d[11])
                }
                res_subcatchment.append(row_data)
        
        cur.execute('SELECT p_id, geom FROM "AquaScope_MVP".points_data_view WHERE LOWER(catchment) = LOWER(%s) ORDER BY id ASC ', (catchment,))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[0],
                    'geom': json.loads(d[1]),
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'catchment': str(res_catchment),
            'subCatchment': str(res_subcatchment),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def wqIndcatorFactApi(catchment_id, indicator_id):
    try:
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".wq_indicator_fact_view WHERE catchment_id = %s AND indicator_id = %s'
        cur.execute(query, (catchment_id, indicator_id))
        data = cur.fetchall()
        if not data:
            res_wq_indcator_fact = "Data Not Found"
        else:
            res_wq_indcator_fact = []
            for d in data:
                row_data = {
                    'id': d[0],
                    'date_field': d[1].strftime("%Y-%m-%d"),
                    'reach': d[2],
                    'value': d[3],
                    'datetimeid': d[4],
                    'c_name': d[5],
                    'indicator_id': d[6],
                    'points_id': d[7],
                    'indicator_name': d[8],
                    'catchment_id': catchment_id
                }
                res_wq_indcator_fact.append(row_data)
            
            
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".wq_indicator_fact_view WHERE catchment_id = %s AND indicator_id = %s) AND c_id = %s'
        cur.execute(query, (catchment_id, indicator_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_points.append(row_data)
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'wq_indcator_fact': str(res_wq_indcator_fact),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def wqIndcatorFactIdApi(catchment_id):
    try:
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".wq_indicator_fact_view WHERE catchment_id = %s '
        cur.execute(query, (catchment_id,))
        data = cur.fetchall()
        if not data:
            res_wq_indcator_fact = "Data Not Found"
        else:
            res_wq_indcator_fact = []
            for d in data:
                row_data = {
                    'id': d[0],
                    'date_field': d[1].strftime("%Y-%m-%d"),
                    'reach': d[2],
                    'value': d[3],
                    'datetimeid': d[4],
                    'c_name': d[5],
                    'indicator_id': d[6],
                    'points_id': d[7],
                    'indicator_name': d[8],
                    'catchment_id': catchment_id,
                }
                res_wq_indcator_fact.append(row_data)
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'wq_indcator_fact': str(res_wq_indcator_fact),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def wqIndcatorDateRangeApi(startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".wq_indicator_fact_view WHERE time_start >= %s AND time_start <= %s AND catchment_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        if not data:
            res_wq_indcator_fact = "Data Not Found"
        else:
            res_wq_indcator_fact = []
            for d in data:
                row_data = {
                    'id': d[0],
                    'date_field': d[1].strftime("%Y-%m-%d"),
                    'reach': d[2],
                    'value': d[3],
                    'datetimeid': d[4],
                    'c_name': d[5],
                    'indicator_id': d[6],
                    'points_id': d[7],
                    'indicator_name': d[8],
                    'catchment_id': catchment_id,
                }
                res_wq_indcator_fact.append(row_data)
            
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".wq_indicator_fact_view WHERE time_start >= %s AND time_start <= %s AND catchment_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_points.append(row_data)
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'wq_indcator_fact': str(res_wq_indcator_fact),
            'point': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def waterQualityLookup(regulatory, water_quality_indicator, catchment_id):
    try:
        cur, conn = connect()
        query = f"""SELECT * FROM "AquaScope_MVP".view_water_quality_lookup WHERE LOWER(regulatory) = LOWER('{regulatory}') AND LOWER(alias) = LOWER('{water_quality_indicator}') AND c_id = {catchment_id}"""
        cur.execute(query)
        data = cur.fetchall()
        if not data:
            res_wq_lookup = "Data Not Found"
        else:
            res_wq_lookup = []
            for d in data:
                row_data = {
                    'description': d[0],
                    'label': d[1],
                    'environment': d[2],
                    'unitofmeasure': d[3],
                    'regulatory': d[4],
                    # 'id': d[5],
                    'alias': d[6],
                    'c_id': d[7],
                }
                res_wq_lookup.append(row_data)
            
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'waterQualityLookup': str(res_wq_lookup),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'waterQualityLookup': json.dumps({'error': 'Internal server error'})
        }
        
def satelliteApi(startDate, endDate, catchment_id):
    try:
        dateType = 'YYYYMMDD'
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".view_meteo_fact WHERE to_date(CAST(datetimeid AS VARCHAR), %s) >= %s AND to_date(CAST(datetimeid AS VARCHAR), %s) <= %s AND c_id = %s'
        cur.execute(query, (dateType, startDate, dateType, endDate, catchment_id))
        data = cur.fetchall()
        if not data:
            res_satellite = "Data Not Found"
        else:
            res_satellite = []
            for d in data:
                row_data = {
                    'id': d[0],
                    'name': d[1],
                    'date': d[2],
                    'surface_soil_moisture': d[3],
                    'subsurface': d[4],
                    'imerg': d[5],
                    'lst': d[6],
                    'c_id': d[7],
                    'datetimeid': d[8],
                }
                res_satellite.append(row_data)
            
        cur, conn = connect()
        
        cur.execute('SELECT * FROM "AquaScope_MVP".catchment_geojson_data WHERE c_id = %s', (catchment_id,))
        data = cur.fetchall()
        if not data:
            res_catchment = "Data Not Found"
        else:
            res_catchment = []
            for d in data:
                row_data = {
                    'c_id': d[0],
                    'catchment': d[1],
                    'geom': json.loads(d[7]),
                }
                res_catchment.append(row_data)
         
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'satelliteData': str(res_satellite),
            'catchment': str(res_catchment),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'satelliteData': json.dumps({'error': 'Internal server error'}),
            'point': json.dumps({'error': 'Internal server error'})
        }
        
def liveSensorDataApi(startDate,endDate, catchment_id):
    try:
        cur, conn = connect()
        query = f"""SELECT * FROM "AquaScope_MVP".view_live_sensor_data2_avg WHERE to_date(date, 'DD/MM/YYYY') >= '{startDate}' AND to_date(date, 'DD/MM/YYYY') <= '{endDate}' AND c_id = {catchment_id} ORDER BY id ASC"""

        cur.execute(query)
        data = cur.fetchall()
        
        if not data:
            result = "Data Not Found"
        else:
            result = []
            distinct_source = set()
            
            for d in data:
                row_data = {
                    'date': d[0],
                    'time': d[1],
                    'temp': d[2],
                    'avg_temp': d[3],
                    'cond': d[4],
                    'avg_cond': d[5],
                    'do_pcent': d[6],
                    'avg_do_pcent': d[7],
                    'do_mgl': d[8],
                    'avg_do_mgl': d[9],
                    'ph': d[10],
                    'avg_ph': d[11],
                    'ammonium': d[12],
                    'avg_ammonium': d[13],
                    'turbidity': d[14],
                    'avg_turbidity': d[15],
                    'doo': d[16],
                    'avg_doo': d[17],
                    'doo_pcent': d[18],
                    'avg_doo_pcent': d[19],
                    'doo_mgl': d[20],
                    'avg_doo_mgl': d[21],
                    'battery': d[22],
                    'avg_battery': d[23],
                    'salinty': d[24],
                    'avg_salinty': d[25],
                    'chlorophyl': d[26],
                    'avg_chlorophyl': d[27],
                    'source': d[28],
                    'c_id': d[29],
                    'id': d[30],
                }
                result.append(row_data)
                distinct_source.add(row_data['source'])
                
        query = f"""SELECT * FROM "AquaScope_MVP".points_data_view WHERE alias IN ({', '.join(f"'{reach}'" for reach in distinct_source)}) AND c_id = {catchment_id}"""
        cur.execute(query)
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'live_sensor_data': str(result),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'live_sensor_data': json.dumps({'error': 'Internal server error'})
        }
        
def userAccessLevelApi(user_ids):
    try:
        cur, conn = connect()
        
        cur.execute('SELECT access_level FROM "AquaScope_MVP"."user" WHERE user_id = %s', (user_ids,))
        data = cur.fetchall()
        result = []
        for d in data:
            row_data = {
                'access_level': d[0],
            }
            result.append(row_data)
            
        cur.close()
        conn.close()
        return {
            'statusCode': 200,
            'access_level': result[0]['access_level'],
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'access_level': json.dumps({'error': 'Internal server error'})
        }
        
def meteoFactApi(startDate,endDate, catchment_id):
    try:
        cur, conn = connect()
        dateType = 'YYYYMMDD'
        query = 'SELECT * FROM "AquaScope_MVP"."Meteo_fact" WHERE to_date(CAST(datetimeid AS VARCHAR), %s) >= %s AND to_date(CAST(datetimeid AS VARCHAR), %s) <= %s AND c_id = %s'

        cur.execute(query, (dateType, startDate, dateType, endDate, catchment_id))
        data = cur.fetchall()
        result = []
        for d in data:
            row_data = {
                'id': d[0],
                'name': d[1],
                'imerg': d[5],
            }
            result.append(row_data)
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'Result': str(result),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'Result': json.dumps({'error': 'Internal server error'})
        }
        
def subcatchmentApi(subcatchment_name, catchment_id):
    try:
        cur, conn = connect()
        
        cur.execute('SELECT * FROM "AquaScope_MVP".catchment_geojson_data WHERE c_id = %s', (catchment_id,))
        data = cur.fetchall()
        res_catchment = []
        for d in data:
            row_data = {
                'c_id': d[0],
                'catchment': d[1],
                'geom': json.loads(d[7]),
            }
            res_catchment.append(row_data)
        
        cur.execute('SELECT * FROM "AquaScope_MVP".view_sub_catchment WHERE sc_alias = %s AND c_id = %s', (subcatchment_name, catchment_id))
        data = cur.fetchall()
        if not data:
            res_subcatchment = "Data Not Found"
        else:
            res_subcatchment = []
            for d in data:
                row_data = {
                    'gid': d[0],
                    'gr_id': d[1],
                    'c_id': d[2],
                    'sc_id': d[3],
                    'subcatchment': d[4],
                    'sc_alias': d[5],
                    'geom': json.loads(d[11]),
                }
                res_subcatchment.append(row_data)
        
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'catchment': str(res_catchment),
            'subcatchment': str(res_subcatchment),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'catchment': "Error while fetching data",
            'subcatchment': "Error while fetching data",
            'body': json.dumps({'error': 'Internal server error'})
        }
                
def incapStatApi(date, aggregate, catchment_id):
    try:
        cur, conn = connect()
        
        if aggregate == "":
            cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s', (catchment_id,))
            data = cur.fetchall()
            if not data:
                res_incap = "Data Not Found"
            else:
                res_incap = []
                for d in data:
                    row_data = {
                        'date_': d[0],
                        'discharge': d[1],
                        'volume': d[2],
                        'velocity': d[3],
                        'depth_': d[4],
                        'stream_power': d[5],
                        'shear_vel': d[6],
                        'max_ent_grain': d[7],
                        'mov_bed_mass': d[8],
                        'ent_rate': d[9],
                        'dep_rate': d[10],
                        'bed_sed': d[11],
                        'stream_ss': d[12],
                        'diffuse_ss': d[13],
                        'wc_tdp': d[14],
                        'wc_pp': d[15],
                        'wc_sorp_rel': d[16],
                        'bed_tdp': d[17],
                        'bed_pp': d[18],
                        'bed_sorp_rel_': d[19],
                        'macro_mass': d[20],
                        'epi_mass': d[21],
                        'wc_tp': d[22],
                        'wc_srp': d[23],
                        'temperature': d[24],
                        'tdp_diffuse': d[25],
                        'pp_diffuse': d[26],
                        'wc_epc0': d[27],
                        'bed_epc0': d[28],
                        'ss_mass': d[29],
                        'mprop': d[30],
                        'ut': d[31],
                        'r': d[32],
                        'rmax': d[33],
                        'phytoplankton': d[34],
                        'do_': d[35],
                        'bod': d[36],
                        'stats': d[37],
                        'reach': d[38],
                        'c_id': d[40],
                    }
                    res_incap.append(row_data)
                    
                query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                cur.execute(query, (catchment_id, catchment_id))
                data = cur.fetchall()
                if not data:
                    res_points = "Data Not Found"
                else:
                    res_points = []
                    for d in data:
                        row_data = {
                            'p_id': d[5],
                            'geom': json.loads(d[10])
                        }
                        res_points.append(row_data)
                    
            
        else:
            if date == "":
                if aggregate.lower() == "month":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_p_output_stats_mnthlyagg_fact WHERE c_id = %s', (catchment_id,))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'date_': d[0],
                                'discharge': d[1],
                                'volume': d[2],
                                'velocity': d[3],
                                'depth_': d[4],
                                'stream_power': d[5],
                                'shear_vel': d[6],
                                'max_ent_grain': d[7],
                                'mov_bed_mass': d[8],
                                'ent_rate': d[9],
                                'dep_rate': d[10],
                                'bed_sed': d[11],
                                'stream_ss': d[12],
                                'diffuse_ss': d[13],
                                'wc_tdp': d[14],
                                'wc_pp': d[15],
                                'wc_sorp_rel': d[16],
                                'bed_tdp': d[17],
                                'bed_pp': d[18],
                                'bed_sorp_rel_': d[19],
                                'macro_mass': d[20],
                                'epi_mass': d[21],
                                'wc_tp': d[22],
                                'wc_srp': d[23],
                                'temperature': d[24],
                                'tdp_diffuse': d[25],
                                'pp_diffuse': d[26],
                                'wc_epc0': d[27],
                                'bed_epc0': d[28],
                                'ss_mass': d[29],
                                'mprop': d[30],
                                'ut': d[31],
                                'r': d[32],
                                'rmax': d[33],
                                'phytoplankton': d[34],
                                'do_': d[35],
                                'bod': d[36],
                                'stats': d[37],
                                'reach': d[38],
                                'c_id': d[40],
                            }
                            res_incap.append(row_data)
                            
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_stats_mnthlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                    
                elif aggregate.lower() == "year":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s', (catchment_id,))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'date_': d[0],
                                'discharge': d[1],
                                'volume': d[2],
                                'velocity': d[3],
                                'depth_': d[4],
                                'stream_power': d[5],
                                'shear_vel': d[6],
                                'max_ent_grain': d[7],
                                'mov_bed_mass': d[8],
                                'ent_rate': d[9],
                                'dep_rate': d[10],
                                'bed_sed': d[11],
                                'stream_ss': d[12],
                                'diffuse_ss': d[13],
                                'wc_tdp': d[14],
                                'wc_pp': d[15],
                                'wc_sorp_rel': d[16],
                                'bed_tdp': d[17],
                                'bed_pp': d[18],
                                'bed_sorp_rel_': d[19],
                                'macro_mass': d[20],
                                'epi_mass': d[21],
                                'wc_tp': d[22],
                                'wc_srp': d[23],
                                'temperature': d[24],
                                'tdp_diffuse': d[25],
                                'pp_diffuse': d[26],
                                'wc_epc0': d[27],
                                'bed_epc0': d[28],
                                'ss_mass': d[29],
                                'mprop': d[30],
                                'ut': d[31],
                                'r': d[32],
                                'rmax': d[33],
                                'phytoplankton': d[34],
                                'do_': d[35],
                                'bod': d[36],
                                'stats': d[37],
                                'reach': d[38],
                                'c_id': d[40],
                            }
                            res_incap.append(row_data)
                            
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                else:
                    res_incap = "Invalid aggregate value try with month or year"
                    
            else:
                year = date.split("-")[0]
                date_object = datetime.strptime(date, "%Y-%m-%d")
                month = date_object.strftime("%b")
        
                if aggregate.lower() == "month":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_p_output_stats_mnthlyagg_fact WHERE c_id = %s AND date_ = %s', (catchment_id, month))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'date_': d[0],
                                'discharge': d[1],
                                'volume': d[2],
                                'velocity': d[3],
                                'depth_': d[4],
                                'stream_power': d[5],
                                'shear_vel': d[6],
                                'max_ent_grain': d[7],
                                'mov_bed_mass': d[8],
                                'ent_rate': d[9],
                                'dep_rate': d[10],
                                'bed_sed': d[11],
                                'stream_ss': d[12],
                                'diffuse_ss': d[13],
                                'wc_tdp': d[14],
                                'wc_pp': d[15],
                                'wc_sorp_rel': d[16],
                                'bed_tdp': d[17],
                                'bed_pp': d[18],
                                'bed_sorp_rel_': d[19],
                                'macro_mass': d[20],
                                'epi_mass': d[21],
                                'wc_tp': d[22],
                                'wc_srp': d[23],
                                'temperature': d[24],
                                'tdp_diffuse': d[25],
                                'pp_diffuse': d[26],
                                'wc_epc0': d[27],
                                'bed_epc0': d[28],
                                'ss_mass': d[29],
                                'mprop': d[30],
                                'ut': d[31],
                                'r': d[32],
                                'rmax': d[33],
                                'phytoplankton': d[34],
                                'do_': d[35],
                                'bod': d[36],
                                'stats': d[37],
                                'reach': d[38],
                                'c_id': d[40],
                            }
                            res_incap.append(row_data)
                    
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_stats_mnthlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                elif aggregate.lower() == "year":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s AND date_ = %s', (catchment_id, year))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'date_': d[0],
                                'discharge': d[1],
                                'volume': d[2],
                                'velocity': d[3],
                                'depth_': d[4],
                                'stream_power': d[5],
                                'shear_vel': d[6],
                                'max_ent_grain': d[7],
                                'mov_bed_mass': d[8],
                                'ent_rate': d[9],
                                'dep_rate': d[10],
                                'bed_sed': d[11],
                                'stream_ss': d[12],
                                'diffuse_ss': d[13],
                                'wc_tdp': d[14],
                                'wc_pp': d[15],
                                'wc_sorp_rel': d[16],
                                'bed_tdp': d[17],
                                'bed_pp': d[18],
                                'bed_sorp_rel_': d[19],
                                'macro_mass': d[20],
                                'epi_mass': d[21],
                                'wc_tp': d[22],
                                'wc_srp': d[23],
                                'temperature': d[24],
                                'tdp_diffuse': d[25],
                                'pp_diffuse': d[26],
                                'wc_epc0': d[27],
                                'bed_epc0': d[28],
                                'ss_mass': d[29],
                                'mprop': d[30],
                                'ut': d[31],
                                'r': d[32],
                                'rmax': d[33],
                                'phytoplankton': d[34],
                                'do_': d[35],
                                'bod': d[36],
                                'stats': d[37],
                                'reach': d[38],
                                'c_id': d[40],
                            }
                            res_incap.append(row_data)
                    
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                else:
                    res_incap = "Invalid aggregate value try with month or year"
                    points = "Invalid aggregate value try with month or year"
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incapStat': str(res_incap),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'incapStat': "Error while fetching data",
            'points': "Error while fetching data",
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def incanStatApi(date, aggregate, catchment_id):
    try:
        cur, conn = connect()
        
        if aggregate == "":
            cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s', (catchment_id,))
            data = cur.fetchall()
            if not data:
                res_incap = "Data Not Found"
            else:
                res_incap = []
                for d in data:
                    row_data = {
                        'index': d[0],
                        'date': d[1],
                        'discharge': d[2],
                        'nitrate': d[3],
                        'ammonium': d[4],
                        'volume': d[5],
                        'stats': d[6],
                        'reach': d[7],
                        'c_id': d[9],
                    }
                    res_incap.append(row_data)
                    
                query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                cur.execute(query, (catchment_id, catchment_id))
                data = cur.fetchall()
                if not data:
                    res_points = "Data Not Found"
                else:
                    res_points = []
                    for d in data:
                        row_data = {
                            'p_id': d[5],
                            'geom': json.loads(d[10])
                        }
                        res_points.append(row_data)
                    
            
        else:
            if date == "":
                if aggregate.lower() == "month":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_n_output_stats_mnthlyagg_fact WHERE c_id = %s', (catchment_id,))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'index': d[0],
                                'date': d[1],
                                'discharge': d[2],
                                'nitrate': d[3],
                                'ammonium': d[4],
                                'volume': d[5],
                                'stats': d[6],
                                'reach': d[7],
                                'c_id': d[9],
                            }
                            res_incap.append(row_data)
                            
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_stats_mnthlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                    
                elif aggregate.lower() == "year":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s', (catchment_id,))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'index': d[0],
                                'date': d[1],
                                'discharge': d[2],
                                'nitrate': d[3],
                                'ammonium': d[4],
                                'volume': d[5],
                                'stats': d[6],
                                'reach': d[7],
                                'c_id': d[9],
                            }
                            res_incap.append(row_data)
                            
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                else:
                    res_incap = "Invalid aggregate value try with month or year"
                    
            else:
                year = date.split("-")[0]
                date_object = datetime.strptime(date, "%Y-%m-%d")
                month = date_object.strftime("%b")
        
                if aggregate.lower() == "month":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_n_output_stats_mnthlyagg_fact WHERE c_id = %s AND date = %s', (catchment_id, month))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'index': d[0],
                                'date': d[1],
                                'discharge': d[2],
                                'nitrate': d[3],
                                'ammonium': d[4],
                                'volume': d[5],
                                'stats': d[6],
                                'reach': d[7],
                                'c_id': d[9],
                            }
                            res_incap.append(row_data)
                    
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_stats_mnthlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                elif aggregate.lower() == "year":
                    cur.execute('SELECT * FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s AND date = %s', (catchment_id, year))
                    data = cur.fetchall()
                    if not data:
                        res_incap = "Data Not Found"
                    else:
                        res_incap = []
                        for d in data:
                            row_data = {
                                'index': d[0],
                                'date': d[1],
                                'discharge': d[2],
                                'nitrate': d[3],
                                'ammonium': d[4],
                                'volume': d[5],
                                'stats': d[6],
                                'reach': d[7],
                                'c_id': d[9],
                            }
                            res_incap.append(row_data)
                    
                    query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_stats_yearlyagg_fact WHERE c_id = %s) AND c_id = %s'
                    cur.execute(query, (catchment_id, catchment_id))
                    data = cur.fetchall()
                    if not data:
                        res_points = "Data Not Found"
                    else:
                        res_points = []
                        for d in data:
                            row_data = {
                                'p_id': d[5],
                                'geom': json.loads(d[10])
                            }
                            res_points.append(row_data)
                        
                    
                else:
                    res_incap = "Invalid aggregate value try with month or year"
                    points = "Invalid aggregate value try with month or year"
            
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incanStat': str(res_incap),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'incanStat': "Error while fetching data",
            'points': "Error while fetching data",
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def incaOutputDsdFact(reaches, startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        query = f"""SELECT * FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= '{startDate}' AND date_ <= '{endDate}' AND c_id = {catchment_id} AND reach IN ({', '.join([f"'{reach}'" for reach in reaches])}) ORDER BY id ASC"""
        cur.execute(query)
        data = cur.fetchall()
        
        if not data:
            res_incapDsd = "Data Not Found"
        else:
            res_incapDsd = []
            for d in data:
                row_data = {
                    'date_': d[0],
                    'discharge': d[1],
                    'volume': d[2],
                    'velocity': d[3],
                    'water_depth': d[4],
                    'stream_power': d[5],
                    'shear_velocity': d[6],
                    'max_ent_grain_size': d[7],
                    'moveable_bed_mass': d[8],
                    'entrainment_rate': d[9],
                    'deposition_rate': d[10],
                    'bed_sediment': d[11],
                    'suspended_sediment': d[12],
                    'diffuse_sediment': d[13],
                    'water_column_tdp': d[14],
                    'water_column_pp': d[15],
                    'wc_sorption_release': d[16],
                    'stream_bed_tdp': d[17],
                    'stream_bed_pp': d[18],
                    'bed_sorption_release': d[19],
                    'macrophyte_mass': d[20],
                    'epiphyte_mass': d[21],
                    'water_column_tp': d[22],
                    'water_column_srp': d[23],
                    'water_temperature': d[24],
                    'tdp_input': d[25],
                    'pp_input': d[26],
                    'water_column_epc0': d[27],
                    'stream_bed_epc0': d[28],
                    'suspended_sediment_mass': d[29],
                    'mprop': d[30],
                    'settling_velocity': d[31],
                    'r': d[32],
                    'rmax': d[33],
                    'live_phytoplankton': d[34],
                    'dissolved_oxygen': d[35],
                    'bod': d[36],
                    '_saturation': d[37],
                    'reach': d[38],
                    'datetimeid': d[39],
                    'c_id': d[41],
                    'use_case': d[42],
                }
                res_incapDsd.append(row_data)
                
        query = f"""SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= '{startDate}' AND date_ <= '{endDate}' AND c_id = {catchment_id} AND reach IN ({', '.join([f"'{reach}'" for reach in reaches])})) AND c_id = {catchment_id}"""
        cur.execute(query)
        data = cur.fetchall()
        if not data:
            res_incapPoints = "Data Not Found"
        else:
            res_incapPoints = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_incapPoints.append(row_data)
                
        cur, conn = connect()
        query = f"""SELECT * FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= '{startDate}' AND date <= '{endDate}' AND c_id = {catchment_id} AND reach IN ({', '.join([f"'{reach}'" for reach in reaches])})  ORDER BY id ASC"""

        cur.execute(query)
        data = cur.fetchall()
        
        if not data:
            res_incanDsd = "Data Not Found"
        else:
            res_incanDsd = []
            for d in data:
                row_data = {
                    'flow': d[0],
                    'nitrate': d[1],
                    'ammonium': d[2],
                    'volume': d[3],
                    'reach': d[4],
                    'date': d[5],
                    'id': d[6],
                    'c_id': d[7],
                    'use_case': d[8],
                }
                res_incanDsd.append(row_data)
                
        query = f"""SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= '{startDate}' AND date <= '{endDate}' AND c_id = {catchment_id} AND reach IN ({', '.join([f"'{reach}'" for reach in reaches])}))  AND c_id = {catchment_id}"""
        cur.execute(query)
        data = cur.fetchall()
        if not data:
            res_incanPoints = "Data Not Found"
        else:
            res_incanPoints = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_incanPoints.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incapOutputDsdFact': str(res_incapDsd),
            'incapOutputDsdFactPoints': str(res_incapPoints),
            'incanOutputDsdFact': str(res_incanDsd),
            'incanOutputDsdFactPoints': str(res_incanPoints),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
       
def incaDsdApi(startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s ORDER BY id ASC'
        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        
        if not data:
            res_incapDsd = "Data Not Found"
        else:
            res_incapDsd = []
            for d in data:
                row_data = {
                    'date_': d[0],
                    'discharge': d[1],
                    'volume': d[2],
                    'velocity': d[3],
                    'water_depth': d[4],
                    'stream_power': d[5],
                    'shear_velocity': d[6],
                    'max_ent_grain_size': d[7],
                    'moveable_bed_mass': d[8],
                    'entrainment_rate': d[9],
                    'deposition_rate': d[10],
                    'bed_sediment': d[11],
                    'suspended_sediment': d[12],
                    'diffuse_sediment': d[13],
                    'water_column_tdp': d[14],
                    'water_column_pp': d[15],
                    'wc_sorption_release': d[16],
                    'stream_bed_tdp': d[17],
                    'stream_bed_pp': d[18],
                    'bed_sorption_release': d[19],
                    'macrophyte_mass': d[20],
                    'epiphyte_mass': d[21],
                    'water_column_tp': d[22],
                    'water_column_srp': d[23],
                    'water_temperature': d[24],
                    'tdp_input': d[25],
                    'pp_input': d[26],
                    'water_column_epc0': d[27],
                    'stream_bed_epc0': d[28],
                    'suspended_sediment_mass': d[29],
                    'mprop': d[30],
                    'settling_velocity': d[31],
                    'r': d[32],
                    'rmax': d[33],
                    'live_phytoplankton': d[34],
                    'dissolved_oxygen': d[35],
                    'bod': d[36],
                    '_saturation': d[37],
                    'reach': d[38],
                    'datetimeid': d[39],
                    'c_id': d[41],
                    'use_case': d[42],
                }
                res_incapDsd.append(row_data)
                
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_incapPoints = "Data Not Found"
        else:
            res_incapPoints = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_incapPoints.append(row_data)
                
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= %s AND date <= %s AND c_id = %s ORDER BY id ASC'

        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        
        if not data:
            res_incanDsd = "Data Not Found"
        else:
            res_incanDsd = []
            for d in data:
                row_data = {
                    'flow': d[0],
                    'nitrate': d[1],
                    'ammonium': d[2],
                    'volume': d[3],
                    'reach': d[4],
                    'date': d[5],
                    'id': d[6],
                    'c_id': d[7],
                    'use_case': d[8],
                }
                res_incanDsd.append(row_data)
                
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= %s AND date <= %s AND c_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_incanPoints = "Data Not Found"
        else:
            res_incanPoints = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_incanPoints.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incapDsd': str(res_incapDsd),
            'incapPoints': str(res_incapPoints),
            'incanDsd': str(res_incanDsd),
            'incanPoints': str(res_incanPoints),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def incapDsdApi(startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        # dateType = 'YYYY-MM-DD'
        query = 'SELECT * FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s ORDER BY id ASC'

        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        
        if not data:
            result = "Data Not Found"
        else:
            result = []
            for d in data:
                row_data = {
                    'date_': d[0],
                    'discharge': d[1],
                    'volume': d[2],
                    'velocity': d[3],
                    'water_depth': d[4],
                    'stream_power': d[5],
                    'shear_velocity': d[6],
                    'max_ent_grain_size': d[7],
                    'moveable_bed_mass': d[8],
                    'entrainment_rate': d[9],
                    'deposition_rate': d[10],
                    'bed_sediment': d[11],
                    'suspended_sediment': d[12],
                    'diffuse_sediment': d[13],
                    'water_column_tdp': d[14],
                    'water_column_pp': d[15],
                    'wc_sorption_release': d[16],
                    'stream_bed_tdp': d[17],
                    'stream_bed_pp': d[18],
                    'bed_sorption_release': d[19],
                    'macrophyte_mass': d[20],
                    'epiphyte_mass': d[21],
                    'water_column_tp': d[22],
                    'water_column_srp': d[23],
                    'water_temperature': d[24],
                    'tdp_input': d[25],
                    'pp_input': d[26],
                    'water_column_epc0': d[27],
                    'stream_bed_epc0': d[28],
                    'suspended_sediment_mass': d[29],
                    'mprop': d[30],
                    'settling_velocity': d[31],
                    'r': d[32],
                    'rmax': d[33],
                    'live_phytoplankton': d[34],
                    'dissolved_oxygen': d[35],
                    'bod': d[36],
                    '_saturation': d[37],
                    'reach': d[38],
                    'datetimeid': d[39],
                    'c_id': d[41],
                    'use_case': d[42],
                }
                result.append(row_data)
                
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_p_output_dsd_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incapDsd': str(result),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def incanDsdApi(startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        query = 'SELECT * FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= %s AND date <= %s AND c_id = %s ORDER BY id ASC'

        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        
        if not data:
            result = "Data Not Found"
        else:
            result = []
            for d in data:
                row_data = {
                    'flow': d[0],
                    'nitrate': d[1],
                    'ammonium': d[2],
                    'volume': d[3],
                    'reach': d[4],
                    'date': d[5],
                    'id': d[6],
                    'c_id': d[7],
                    'use_case': d[8],
                }
                result.append(row_data)
                
        query = 'SELECT * FROM "AquaScope_MVP".points_data_view WHERE p_id IN (SELECT DISTINCT reach FROM "AquaScope_MVP".view_inca_n_output_dsd_fact WHERE date >= %s AND date <= %s AND c_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'p_id': d[5],
                    'geom': json.loads(d[10])
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incanDsd': str(result),
            'points': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
def incapTcApi(startDate, endDate, catchment_id):
    try:
        cur, conn = connect()
        # dateType = 'YYYY-MM-DD'
        query = 'SELECT * FROM "AquaScope_MVP".view_inca_p_output_tc_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s ORDER BY id ASC'

        cur.execute(query, (startDate, endDate, catchment_id))
        data = cur.fetchall()
        
        if not data:
            result = "Data Not Found"
        else:
            result = []
            for d in data:
                row_data = {
                    'date_': d[0],
                    'direct_runoff': d[1],
                    'direct_pp': d[2],
                    'direct_tdp': d[3],
                    'flow_erosion': d[4],
                    'transport_capacity': d[5],
                    'k_': d[6],
                    'splash_detachment': d[7],
                    'sediment_store': d[8],
                    'sediment_out': d[9],
                    'soil_ad_desorption': d[10],
                    'soil_labile_p': d[11],
                    'soil_inactive_p': d[12],
                    'soil_epc0': d[13],
                    'soil_mass': d[14],
                    'soil_water_flow': d[15],
                    'soil_water_volume': d[16],
                    'soil_water_tdp': d[17],
                    'soil_water_bod': d[18],
                    'groundwater_flow': d[19],
                    'groundwater_volume': d[20],
                    'groundwater_tdp': d[21],
                    'groundwater_ad_desorption': d[22],
                    'groundwater_total_solid_p': d[23],
                    'groundwater_epc0': d[24],
                    'groundwater_bod': d[25],
                    'sub_catchment': d[26],
                    'datetimeid': d[27],
                    'c_id': d[29],
                }
                result.append(row_data)
                
        query = 'SELECT * FROM "AquaScope_MVP".view_sub_catchment WHERE subcatch IN (SELECT DISTINCT sub_catchment FROM "AquaScope_MVP".view_inca_p_output_tc_fact WHERE date_ >= %s AND date_ <= %s AND c_id = %s) AND c_id = %s'
        cur.execute(query, (startDate, endDate, catchment_id, catchment_id))
        data = cur.fetchall()
        if not data:
            res_points = "Data Not Found"
        else:
            res_points = []
            for d in data:
                row_data = {
                    'subcatch': d[4],
                    'catchment': d[10],
                    'geom': json.loads(d[11])
                }
                res_points.append(row_data)
                
        cur.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'incapTc': str(result),
            'subCatchment': str(res_points),
        }
        
    except Exception as e:
        print('Error fetching data:', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }
        
        
        
        