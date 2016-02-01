
import psycopg2

from vpp.database.db_manager import DBManager


def run():
    result = fetch_sensors_from_gandalf()
    process_sensors(result)

def process_sensors(result):
    db_manager = DBManager()
    existing_count = 0
    created_count = 0
    for grundfos_id, description, unit in result:
        sensor_id = 'grundfos_' + str(grundfos_id)
        existing = db_manager.get_device(sensor_id)
        if existing is None:
            db_manager.create_new_sensor(sensor_id, description, unit)
            created_count += 1
        else:
            existing_count += 1
        if created_count%100 == 0 or existing_count%100 == 0:
            print "Progress: Created " + str(created_count) + " sensors, skipped " + str(existing_count) + " existing."
    db_manager.close()
    print "Finished: Created " + str(created_count) + " sensors, skipped " + str(existing_count) + " existing."

def fetch_sensors_from_gandalf():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""SELECT sensor_id, description, unit FROM sensorscharacteristics""")
    result = cursor.fetchall()
    conn.close()
    return result


def get_connection():
    return psycopg2.connect("dbname='RollingDbProduction' host='gandalf.netlab.eng.au.dk' user='ubbe' password='vpp4sgr2015'")


#def fetch_additional_sensors_from_gandalf():
#    connection = get_connection()
#    cursor = connection.cursor()



if __name__ == "__main__":
    run()