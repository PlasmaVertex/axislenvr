import sqlite3
import sys
import datetime
import time
import os


def round_datetime_to_nearest_quarter(dt):
    # Round down to the nearest 15 minute interval
    dt -= datetime.timedelta(minutes=dt.minute % 15,
                             seconds=dt.second,
                             microseconds=dt.microsecond)
    return dt


def datetime_from_utc_to_local(utc_datetime):
    epoch = time.mktime(utc_datetime.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return utc_datetime + offset


def get_file_name(start_date):
    dt_quarter = round_datetime_to_nearest_quarter(start_date)
    localtime = datetime_from_utc_to_local(dt_quarter)
    filename = localtime.strftime('%Y%m%dT%H%M%S') + '.mkv'
    return filename


def register_motion_file(target_file_name, cursor_a, motion_file_param):
    insert_query = """INSERT INTO movement_file(event_from,event_to,duration_in_secs, file_name)
      VALUES(?,?,?,?);"""
    data_tuple = (get_iso_date_string(motion_file_param['startDate']),
                  get_iso_date_string(motion_file_param['endDate']),
                  motion_file_param['durationInSecs'], target_file_name)
    cursor_a.execute(insert_query, data_tuple)

    update_query = """UPDATE event_log SET processed_at = ?
              WHERE event_time > ? AND event_time <= ?;"""

    data_tuple = (get_iso_date_string(datetime.datetime.utcnow()),
                  get_iso_date_string(motion_file_param['startDate'].replace(microsecond=0)),
                  get_iso_date_string(motion_file_param['endDate']))
    cursor_a.execute(update_query, data_tuple)

    sqliteConnection.commit()


def get_iso_date_string(date):
    result = date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return result


def create_motion_file(motionfile):
    source_file = motionfile['sourceFileName']
    source_filename_no_extension = os.path.splitext(source_file)[0]
    if not os.path.exists(source_file):
        source_file = source_filename_no_extension[:-1] + '1' + '.mkv'

    local_start = datetime_from_utc_to_local(motionfile['startDate'])
    local_start_str = local_start.strftime('%H%M%S')

    file_start_local = datetime.datetime.fromisoformat(
        source_filename_no_extension[0:4] + '-' + source_filename_no_extension[4:6]
        + '-' + source_filename_no_extension[6:8] + 'T' + source_filename_no_extension[9:11]
        + ':' + source_filename_no_extension[11:13] + ':' + source_filename_no_extension[13:15] + '+00:00')

    local_end = datetime_from_utc_to_local(motionfile['endDate'])
    local_end_str = local_end.strftime('%H%M%S')
    filedate = file_start_local.strftime('%Y%m%d')
    target_file_name = '{}_{}_{}.mkv'.format(source_filename_no_extension, local_start_str,
                                             local_end_str)
    start_seconds = round((local_start - file_start_local).total_seconds())
    if start_seconds > 2:
        start_seconds -= 2
    duration_seconds = (local_end - local_start).total_seconds()
    duration_seconds = start_seconds + round(duration_seconds + 2)
    target_dir = '{}_{}'.format(target_dir_prefix, filedate)
    command = 'ffmpeg -ss {} -i {} -t {} -c copy -copyts {}/{}'.format(
        start_seconds, source_file, duration_seconds, target_dir, target_file_name)

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    if not os.path.exists(source_file):
        return None
    os.system(command)

    return target_file_name


if __name__ == '__main__':
    # total arguments
    n = len(sys.argv)
    if n < 3:
        print("Total arguments passed:", n)
        print("Usage: dbFile targetdir")
        exit(1)
    db_filename = sys.argv[1]
    target_dir_prefix = sys.argv[2]

    sqliteConnection = None
    try:
        sqliteConnection = sqlite3.connect(db_filename)
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")

        sqlite_event_select_query = """SELECT event_time, event_type, event_data_name, event_data_value 
        FROM event_log        
        WHERE processed_at is NULL"""

        cursor.execute(sqlite_event_select_query)

        rows = cursor.fetchall()
        startStr = ''
        endStr = ''
        endCount = 0
        movements = []
        for row in rows:
            if row[1] != 'Changed':
                continue
            if row[2] != 'active':
                continue

            if row[3] == 1:
                startStr = row[0]
            if row[3] == 0:
                endStr = row[0]
                endCount += 1

            if endCount > 1:
                movements.append({'start': startStr,
                                  'end': endStr})
                endCount = 0

        cursor.close()
        prevStartDate = datetime.datetime.min
        prevEndDate = datetime.datetime.min
        for movement in movements:
            startDate = datetime.datetime.fromisoformat(movement['start'].replace('Z', '+00:00'))
            endDate = datetime.datetime.fromisoformat(movement['end'].replace('Z', '+00:00'))

            movement['startDate'] = startDate
            movement['endDate'] = endDate
            movement['durationInSecs'] = (endDate - startDate).total_seconds()

            if prevStartDate == datetime.datetime.min:
                movement['prevStartInSec'] = 0
            else:
                movement['prevStartInSecs'] = (startDate - prevStartDate).total_seconds()

            if prevEndDate == datetime.datetime.min:
                movement['prevDistanceInSec'] = 0
            else:
                movement['prevDistanceInSec'] = (startDate - prevEndDate).total_seconds()

            prevStartDate = startDate
            prevEndDate = endDate
        motionFiles = []
        newFileBegins = True
        startDate = None
        endDate = None
        for movement in movements:
            if newFileBegins:
                startDate = movement['startDate']
                endDate = movement['endDate']
                newFileBegins = False
            # we need to add a new file
            # when the time elapsed between the start of the current movement
            # and the end of the previous movement is more than 59 secs
            if movement['prevDistanceInSec'] > 59:
                motionFiles.append({'startDate': startDate,
                                    'endDate': endDate,
                                    'durationInSecs': (endDate - startDate).total_seconds(),
                                    'sourceFileName': get_file_name(startDate)})
                newFileBegins = True
            else:
                endDate = movement['endDate']

        print('MotionFiles found:', len(motionFiles))
        cursor = sqliteConnection.cursor()
        for motion_file in motionFiles:
            targetFileName = create_motion_file(motion_file)
            if targetFileName:
                register_motion_file(targetFileName, cursor, motion_file)

    except sqlite3.Error as error:
        print("Failed to select data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
