import xml.etree.ElementTree as ElementTree
import sqlite3
import sys


def read_data_from_xml(xml):

    # parse the XML string
    root = ElementTree.fromstring(xml)

    # find the element with the tag 'element'
    event_elem = root.find('tt:Event', namespaces={'tt': 'http://www.onvif.org/ver10/schema'})
    if not event_elem:
        return {}

    notify_message_elem = event_elem.find('wsnt:NotificationMessage',
                                          namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'})

    message_elem = notify_message_elem.find('wsnt:Message',
                                            namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'})

    msg_elem = message_elem.find('tt:Message', namespaces={'tt': 'http://www.onvif.org/ver10/schema'})

    utc_time = msg_elem.attrib['UtcTime']

    property_operation = msg_elem.attrib['PropertyOperation']

    data_elem = msg_elem.find('tt:Data', namespaces={'tt': 'http://www.onvif.org/ver10/schema'})

    simple_item_elem = data_elem.find('tt:SimpleItem', namespaces={'tt': 'http://www.onvif.org/ver10/schema'})

    data_name = simple_item_elem.attrib['Name']
    data_value = simple_item_elem.attrib['Value']
    # print the text value of the element
    print(utc_time)
    result = {'utcTime': utc_time,
              'propertyOperation': property_operation,
              'dataName': data_name,
              'dataValue': data_value}
    return result


def save_event_data(event_data, cursor_a):
    if len(event_data) == 0:
        return

    insert_query = """INSERT INTO event_log(event_time,event_type,event_data_name,event_data_value)
  VALUES(?,?,?,?)
  ON CONFLICT(event_time) DO UPDATE SET
    event_type=excluded.event_type,
    event_data_name=excluded.event_data_name,
    event_data_value=excluded.event_data_value;"""

    data_tuple = (event_data['utcTime'],
                  event_data['propertyOperation'],
                  event_data['dataName'],
                  event_data['dataValue'])
    cursor_a.execute(insert_query, data_tuple)


if __name__ == '__main__':
    # total arguments
    n = len(sys.argv)
    if n < 3:
        print("Total arguments passed:", n)
        print("Usage: sourceFile targetDb")
        exit(1)
    text_filename = sys.argv[1]
    db_filename = sys.argv[2]
    sqliteConnection = None
    try:
        sqliteConnection = sqlite3.connect(db_filename)
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")

        sqlite_create_table_query = """CREATE TABLE IF NOT EXISTS event_log (
    event_time TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_data_name TEXT NOT NULL,
    event_data_value INTEGER NOT NULL,
    processed_at TEXT NULL 
);"""

        sqlite_create_movement_table_query = """CREATE TABLE IF NOT EXISTS movement_file (
            movement_id INTEGER PRIMARY KEY,
            event_from TEXT NOT NULL,
            event_to TEXT NOT NULL,
            file_name TEXT NOT NULL,
            duration_in_secs  INTEGER NOT NULL
        );"""

        cursor.execute(sqlite_create_table_query)
        cursor.execute(sqlite_create_movement_table_query)
        sqliteConnection.commit()

        file1 = open(text_filename, 'r', encoding='utf8')
        count = 0
        currentXmlPart = ""
        while True:

            # Get next line from file
            line = file1.readline()

            # if line is empty
            # end of file is reached
            if not line:
                break

            currentXmlPart += line
            if line.endswith('</tt:MetadataStream>\n'):
                eventData = read_data_from_xml(currentXmlPart)
                save_event_data(eventData, cursor)
                count += 1
                currentXmlPart = ''

        file1.close()
        sqliteConnection.commit()
        print("Rows inserted successfully into event_log table:", count)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
