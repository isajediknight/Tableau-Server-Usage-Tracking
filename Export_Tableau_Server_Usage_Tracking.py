# Copyright (C) 02/29/2016, Luke Brady, Cerner Corporation
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Joins used from:
# http://onlinehelp.tableau.com/current/server/en-us/help.htm#data_dictionary.html
#
def write_postgres_table(rows,filename,delimeter,header,write_header=True):
    """
    Write out the result rows of a query.  The output file is overwritten.
    """
    from os import getcwd
    import datetime
    outfile = open(getcwd()+'\\output_files\\'+filename,'w')
    if(write_header):
        outfile.write(header+'\n')
    for x in range(len(rows)):
        temp = ''
        for y in range(len(rows[x])):
            # These type checks allow the rows to be written out as strings.
            # string
            if(type('') == type(rows[x][y])):
                temp += rows[x][y] + delimeter
            # datetime
            elif(type(datetime.datetime.now()) == type(rows[x][y])):
                temp += str(rows[x][y]) + delimeter
            # integer
            elif(type(0) == type(rows[x][y])):
                temp += str(rows[x][y]) + delimeter
            # None
            elif(type(None) == type(rows[x][y])):
                temp += delimeter
            else:
                temp += delimeter
        outfile.write(temp + '\n')
    outfile.close()

def append_postgres_table(rows,filename,delimeter):
    """
    Write out the result rows of a query.  Append the output to a file.
    """
    from os import getcwd
    import datetime
    outfile = open(getcwd()+'\\output_files\\'+filename,'a')
    for x in range(len(rows)):
        temp = ''
        for y in range(len(rows[x])):
            # These type checks allow the rows to be written out as strings.
            # string
            if(type('') == type(rows[x][y])):
                temp += rows[x][y] + delimeter
            # datetime
            elif(type(datetime.datetime.now()) == type(rows[x][y])):
                temp += str(rows[x][y]) + delimeter
            # integer
            elif(type(0) == type(rows[x][y])):
                temp += str(rows[x][y]) + delimeter
            # None
            elif(type(None) == type(rows[x][y])):
                temp += delimeter
            else:
                temp += delimeter
        # the [:-1] here removes the last delimeter which we do not want
        outfile.write(temp[:-1] + '\n')
    outfile.close()

def get_all_rows_from_postgres_table(details,attempts,wait_to_retry,trim_results=False):
    """
    Get all the rows of a Postgres Table.
    details: contains the connect info in a namedtuple
    servers_to_try: list of servers to connect to in an array
    attempts: how many attempts do you want to try?
    """
    import psycopg2
    import time
    from os import getcwd
    import datetime
    from datetime import date, timedelta
    counter = -1
    connected = False
    rows = []
    new_arr = []
    inner_arr = []
    for x in range(0,attempts):
        counter += 1
        try_connection = "dbname="+details[counter % len(details)].dbname+" user="+details[counter % len(details)].user
        try_connection += " host="+details[counter % len(details)].host+" password="+details[counter % len(details)].password
        try_connection += " port="+details[counter % len(details)].port+""
        try:
            # Try to connect and execute the query
            conn = psycopg2.connect(try_connection)
            cur = conn.cursor()
            cur.execute('SELECT * from '+details[counter % len(details)].table)

            # Get all the results
            rows = cur.fetchall()
            
            # Close the connection
            cur.close()
            
            # If we make it this far we've made a successful connection
            connected = True
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Successfully connected to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Execute Query\n')
            outfile.close()
            del outfile
        except:
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Failed to connect to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Execute Query\n')
            outfile.close()
            del outfile
            time.sleep(wait_to_retry)
        # If we successfully connected we don't need to keep trying to connect
        if(connected):
            break
    if(trim_results):
        for x in range(len(rows)):
            inner_arr = []
            for y in range(len(rows[x])):
                if(type(rows[x][y]) == type('')):
                    inner_arr.append(str(rows[x][y]).replace('\n',' ').strip())
                else:
                    inner_arr.append(str(rows[x][y]))
            new_arr.append(inner_arr)
        rows = new_arr
    
    return rows

def execute_query_from_postgres_table(details,query,attempts,wait_to_retry,trim_results=False):
    """
    Get the results of a query againsr Postgres Table and write it out to a file.
    """
    import psycopg2
    import time
    from os import getcwd
    import datetime
    from datetime import date, timedelta
    counter = -1
    connected = False
    rows = []
    new_arr = []
    inner_arr = []
    for x in range(0,attempts):
        counter += 1
        try_connection = "dbname="+details[counter % len(details)].dbname+" user="+details[counter % len(details)].user
        try_connection += " host="+details[counter % len(details)].host+" password="+details[counter % len(details)].password
        try_connection += " port="+details[counter % len(details)].port+""
        try:
            # Try to connect and execute the query
            conn = psycopg2.connect(try_connection)
            cur = conn.cursor()
            cur.execute(query)

            # Get all the results
            rows = cur.fetchall()

            # Close the connection
            cur.close()
            
            # If we make it this far we've made a successful connection
            connected = True
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Successfully connected to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Execute Query\n')
            outfile.close()
            del outfile
        except:
            # Unable to connect to the postgres DB
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Failed to connect to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Execute Query\n')
            outfile.close()
            del outfile
            time.sleep(wait_to_retry)

        # If we made a successfull connection we don't need to try again
        if(connected):
            break

    if(trim_results):
        for x in range(len(rows)):
            inner_arr = []
            for y in range(len(rows[x])):
                if(type(rows[x][y]) == type('')):
                    inner_arr.append(str(rows[x][y]).replace('\n',' ').strip())
                else:
                    inner_arr.append(str(rows[x][y]))
            new_arr.append(inner_arr)
        rows = new_arr
    
    return rows

def get_column_names_from_postgres_table(details,delimeter,attempts,wait_to_retry):
    """
    Returns the column names of a table
    """
    import psycopg2
    import time
    from os import getcwd
    import datetime
    from datetime import date, timedelta
    counter = -1
    connected = False
    rows = []
    for x in range(0,attempts):
        counter += 1
        try_connection = "dbname="+details[counter % len(details)].dbname+" user="+details[counter % len(details)].user
        try_connection += " host="+details[counter % len(details)].host+" password="+details[counter % len(details)].password
        try_connection += " port="+details[counter % len(details)].port+""
        try:
            conn = psycopg2.connect(try_connection)
            cur = conn.cursor()
            cur.execute("select column_name from information_schema.columns where table_name="+"'"+details[counter % len(details)].table+"'")
            rows = cur.fetchall()
            cur.close()
            connected = True
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Successfully connected to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Get Field Names\n')
            outfile.close()
            del outfile
        except:
            outfile = open(getcwd()+'\\output_files\\Tableau_Server_Usage_Backup_Log.txt','a')
            outfile.write(str(datetime.datetime.now())+'|Failed to connect to: '+details[counter % len(details)].host+'|Attempt ')
            outfile.write(str(counter)+' of '+str(attempts)+'|Table: '+details[counter % len(details)].table+'|Action: Get Field Names\n')
            outfile.close()
            del outfile
            time.sleep(wait_to_retry)
        if(connected):
            break
    
    temp = ''
    for x in range(len(rows)):
        temp += str(rows[x]) + delimeter
    temp = temp.replace("'",'').replace("(",'').replace(")",'').replace(",",'')
    return temp[:-1]

def find_all(a_str, sub):
    """
    Returns the indexes of {sub} where they were found in {a_str}.  The values
    returned from this function should be made into a list() before they can
    be easily used.
    """
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += 1

def run():
    """
    Export postgres tables for Tableau Server
    """
    from collections import namedtuple
    import datetime
    from datetime import date, timedelta
    from shutil import copyfile
    import time
    from os import getcwd

    # If this is set to True then an incorrect parameter has been entered
    close_program = False

    # parameters[0] : postgres database
    # parameters[1] : postgres username
    # parameters[2] : postgres password
    # parameters[3] : postgres port
    # parameters[4] : Tableau Server Worker 1
    # parameters[5] : Tableau Server Worker 2
    # parameters[6] : Tableau Server Worker 3
    # parameters[7] : Tableau Server Worker 4
    # parameters[8] : GMT or UTC hour difference
    # parameters[9] : Connection attempts
    # parameters[10] : Seconds to wait to retry
    # parameters[11] : Local File Location
    # parameters[12] : Shared Drive File Location | leave blank if you do not want the files copied
    # parameters[13] : Delimter for the output files
    parameters = []
    readfile = open(getcwd()+'\\input_files\\parameters.txt','r')
    for line in readfile:
            backet_open = list(find_all(line,'{'))
            backet_close = list(find_all(line,'}'))
            if(len(backet_close) > 0):
                # We are inputing the parameters
                parameters.append(str(line[backet_close[0]+1:]).strip())
            else:
                # We are skipping the comments section of the parameter file
                pass
    readfile.close()

    try:
        temp = int(parameters[3])
    except:
        close_program = True
        print("{postgres port}Needs to be an integer")

    # Check to make sure all four Tableau Server Worker Names are populated.
    # If they are not, simply add the first one we have to all 4.  (Maybe this is overkill)

    if(len(parameters[4]) < 2 or parameters[4] == '' or type(parameters[4]) == type(None)):
        close_program = True
        print("{Tableau Server Worker 1}Needs to be entered")

    if(len(parameters[5]) == 0):
        parameters[5] = parameters[4]
    if(len(parameters[6]) == 0):
        parameters[6] = parameters[4]
    if(len(parameters[7]) == 0):
        parameters[7] = parameters[4]

    try:
        parameters[8] = int(parameters[8])
    except:
        close_program = True
        print("{GMT or UTC hour difference}Needs to be an integer")

    try:
        parameters[9] = int(parameters[9])
    except:
        close_program = True
        print("{Connection attempts}Needs to be an integer")

    try:
        parameters[10] = int(parameters[10])
    except:
        close_program = True
        print("{Seconds to wait to retry}Needs to be an integer")

    # Check to make sure there is an ending slash for the path
    if(parameters[11][-1] != '/'):
        parameters[11] = parameters[11] + '/'
    if(parameters[12][-1] != '/'):
        parameters[12] = parameters[12] + '/'

    # Catch for any checks that did not pass
    if(close_program):
        print("Please edit paramters.txt to fix the above errors.")
        return "Closed with Errors"

    today = date.today()
    yesterday = date.today() - timedelta(1)
    day_before_yesterday = date.today() - timedelta(2)
    
    # Attmept to take into account time changes.  This is difficult to test.
    # I apologize for those who do not use U.S. time.  If you don't use U.S. time change you will need to edit this line
    DST_offset = time.localtime().tm_isdst

    values = 'dbname user host password port table'
    nt = namedtuple('postgres_table',values)
    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'_http_requests'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'_http_requests'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'_http_requests'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'_http_requests'))
    http_requests_conn = temp
    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'_users'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'_users'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'_users'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'_users'))
    users_conn = temp
    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'_views'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'_views'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'_views'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'_views'))
    views_conn = temp
    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'_workbooks'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'_workbooks'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'_workbooks'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'_workbooks'))
    workbooks_conn = temp
    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'DATA_CONNECTIONS'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'DATA_CONNECTIONS'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'DATA_CONNECTIONS'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'DATA_CONNECTIONS'))
    extract_conn = temp

    my_today = str(today.month)+'/'+str(today.day)+'/'+str(today.year)
    my_yesterday = str(yesterday.month)+'/'+str(yesterday.day)+'/'+str(yesterday.year)

    query_http = 'SELECT controller,action,http_referer,http_user_agent,http_request_uri,remote_ip,'
    query_http += "created_at - INTERVAL '"+str(6 - DST_offset)+" hours'"+' as "created_at",session_id,'
    query_http += "completed_at - INTERVAL '"+str(6 - DST_offset)+" hours'"+' as "completed_at",port,user_id,worker,vizql_session,'
    query_http += 'user_ip,currentsheet,site_id FROM _http_requests WHERE'
    query_http += "((COMPLETED_AT - INTERVAL '"+str(6 - DST_offset)+" hours') > TO_DATE('"+my_yesterday+"','MM/DD/YYYY'))"
    query_http += "AND ((COMPLETED_AT - INTERVAL '"+str(6 - DST_offset)+" hours') < TO_DATE('"+my_today+"','MM/DD/YYYY'))"

    query_logins = "SELECT name,login_at - INTERVAL '"+str(6 - DST_offset)+" hours' as "
    query_logins += '"login_at",friendly_name,site_id FROM _users WHERE login_at IS NOT NULL AND '
    query_logins += "((login_at - INTERVAL '"+str(6 - DST_offset)+" hours') > TO_DATE('"+my_yesterday+"','MM/DD/YYYY'))"
    query_logins += "AND ((login_at - INTERVAL '"+str(6 - DST_offset)+" hours') < TO_DATE('"+my_today+"','MM/DD/YYYY'))"

    query_extract = 'SELECT * FROM ((SELECT D5.ID AS "A_WB_ID",D4.caption AS "Caption (Conn WB)",'
    query_extract += 'D4.created_at AS "Date - Created At (Conn WB)",D4.dbclass AS "DB Class (Conn WB)",'
    query_extract += 'D4.dbname AS "DB Name (Conn WB)",D4.has_extract AS "Has Extract (Conn WB)",'
    query_extract += 'D4.id AS "ID - (Conn WB)",D4.keychain AS "Keychain (Conn WB)",'
    query_extract += 'D4.luid AS "LUID (Conn WB)",D4.name AS "Name (Conn WB)",'
    query_extract += 'D4.owner_id AS "ID - Owner (Conn WB)",D4.owner_type AS "Owner Type (Conn WB)",'
    query_extract += 'D4.password AS "Password (Conn WB)",D4.port AS "Port (Conn WB)",'
    query_extract += 'D4.server AS "Server (Conn WB)",D4.site_id AS "ID - Site (Conn WB)",'
    query_extract += 'D4.tablename AS "Table Name (Conn WB)",D4.updated_at AS "Date - Updated At (Conn WB)",'
    query_extract += 'D4.username AS "Username (Conn WB)",D5.created_at AS "Date - Created At (WB DS)",'
    query_extract += 'D5.id AS "ID - (WB DS)",D5.name AS "Name (WB DS)",'
    query_extract += 'D5.owner_id AS "ID - Owner (WB DS)",D5.owner_name AS "Owner Name (WB DS)",'
    query_extract += 'D5.project_id AS "ID - Project (WB DS)",D5.project_name AS "Project Name (WB DS)",'
    query_extract += 'D5.site_id AS "ID - Site (WB DS)",D5.size AS "Size (WB DS)",'
    query_extract += 'D5.system_user_id AS "ID - System User (WB DS)",D5.updated_at AS "Date - Updated At (WB DS)",'
    query_extract += 'D5.view_count AS "View Count (WB DS)",D5.workbook_url AS "Workbook URL (WB DS)"'
    query_extract += ' FROM DATA_CONNECTIONS D4 INNER JOIN _WORKBOOKS D5 ON D5.ID = D4.OWNER_ID'
    query_extract += " WHERE D4.OWNER_TYPE = 'Workbook') AS A"
    query_extract += ' LEFT JOIN ( SELECT D2.asset_key_id AS "Asset Key (DS)",'
    query_extract += 'D2.connectable AS "Connectable (DS)",D2.content_version AS "Content Version (DS)",'
    query_extract += 'D2.created_at AS "Date - Created (DS)",D2.data_engine_extracts AS "Data Engine Extracts (DS)",'
    query_extract += 'D2.db_class AS "DB Class (DS)",D2.db_name AS "DB Name (DS)",'
    query_extract += 'D2.description AS "Description (DS)",D2.document_version AS "Document Version (DS)",'
    query_extract += 'D2.embedded AS "Embedded (DS)",D2.extracts_incremented_at AS "Date - Extracts Incremented At (DS)",'
    query_extract += 'D2.extracts_refreshed_at AS "Date - Extracts Refreshed At (DS)",'
    query_extract += 'D2.first_published_at AS "Date - First Published At (DS)",'
    query_extract += 'D2.id AS "ID - (DS)",D2.incrementable_extracts AS "Incrementable Extracts (DS)",'
    query_extract += 'D2.is_hierarchical AS "Is Hierarchical (DS)",D2.lock_version AS "Lock Version (DS)",'
    query_extract += 'D2.luid AS "LUID (DS)",D2.name AS "Name (DS)",D2.owner_id AS "ID - Owner (DS)",'
    query_extract += 'D2.project_id AS "ID - Project (DS)",D2.refreshable_extracts AS "Refreshable Extracts (DS)",'
    query_extract += 'D2.repository_data_id AS "ID - Rep Data (DS)",'
    query_extract += 'D2.repository_extract_data_id AS "ID - Rep Extract Data (DS)",'
    query_extract += 'D2.repository_url AS "Repository URL (DS)",D2.revision AS "Revisions (DS)",'
    query_extract += 'D2.site_id AS "ID - Site (DS)",D2.size AS "Size (DS)",'
    query_extract += 'D2.state AS "Sate (DS)",D2.table_name AS "Table Name (DS)",'
    query_extract += 'D2.updated_at AS "Date - Updated At (DS)",D4.caption AS "Caption (Conn DS)",'
    query_extract += 'D4.created_at AS "Date - Created At (Conn DS)",'
    query_extract += 'D4.dbclass AS "DB Class (Conn DS)",D4.dbname AS "DB Name (Conn DS)",'
    query_extract += 'D4.has_extract AS "Has Extract (Conn DS)",D4.id AS "ID - (Conn DS)",'
    query_extract += 'D4.keychain AS "Keychain (Conn DS)",D4.luid AS "LUID (Conn DS)",'
    query_extract += 'D4.name AS "Name (Conn DS)",D4.owner_id AS "ID - Owner (Conn DS)",'
    query_extract += 'D4.owner_type AS "Owner Type (Conn DS)",D4.password AS "Password (Conn DS)",'
    query_extract += 'D4.port AS "Port (Conn DS)",D4.server AS "Server (Conn DS)",'
    query_extract += 'D4.site_id AS "ID - Site (Conn DS)",D4.tablename AS "Table Name (Conn DS)",'
    query_extract += 'D4.updated_at AS "Date - Updated At (Conn DS)",D4.username AS "Username (Conn DS)"'
    query_extract += ' FROM DATASOURCES D2 INNER JOIN DATA_CONNECTIONS D4 ON D2.ID = D4.OWNER_ID'
    query_extract += " WHERE D4.OWNER_TYPE = 'Datasource') B"
    query_extract += ' ON A."DB Class (Conn WB)" = B."DB Class (Conn DS)")'

    # Get the data
    http_requests_rows = execute_query_from_postgres_table(http_requests_conn,query_http,parameters[9],parameters[10])
    user_logins_rows = execute_query_from_postgres_table(users_conn,query_logins,parameters[9],parameters[10])
    users_rows = get_all_rows_from_postgres_table(users_conn,parameters[9],parameters[10])
    views_rows = get_all_rows_from_postgres_table(views_conn,parameters[9],parameters[10])
    workbooks_rows = get_all_rows_from_postgres_table(workbooks_conn,parameters[9],parameters[10])

    # Get headers
    users_header = get_column_names_from_postgres_table(users_conn,parameters[13],parameters[9],parameters[10])
    views_header = get_column_names_from_postgres_table(views_conn,parameters[13],parameters[9],parameters[10])
    workbooks_header = get_column_names_from_postgres_table(workbooks_conn,parameters[13],parameters[9],parameters[10])
    extract_header = 'A_WB_ID|Caption (Conn WB)|Date - Created At (Conn WB)|DB Class (Conn WB)|DB Name (Conn WB)|'
    extract_header += 'Has Extract (Conn WB)|ID - (Conn WB)|Keychain (Conn WB)|LUID (Conn WB)|Name (Conn WB)|'
    extract_header += 'ID - Owner (Conn WB)|Owner Type (Conn WB)|Password (Conn WB)|Port (Conn WB)|'
    extract_header += 'Server (Conn WB)|ID - Site (Conn WB)|Table Name (Conn WB)|Date - Updated At (Conn WB)|'
    extract_header += 'Username (Conn WB)|Date - Created At (WB DS)|ID - (WB DS)|Name (WB DS)|ID - Owner (WB DS)|'
    extract_header += 'Owner Name (WB DS)|ID - Project (WB DS)|Project Name (WB DS)|ID - Site (WB DS)|'
    extract_header += 'Size (WB DS)|ID - System User (WB DS)|Date - Updated At (WB DS)|View Count (WB DS)|'
    extract_header += 'Workbook URL (WB DS)|Asset Key (DS)|Connectable (DS)|Content Version (DS)|Date - Created (DS)|'
    extract_header += 'Data Engine Extracts (DS)|DB Class (DS)|DB Name (DS)|Description (DS)|Document Version (DS)|'
    extract_header += 'Embedded (DS)|Date - Extracts Incremented At (DS)|Date - Extracts Refreshed At (DS)|'
    extract_header += 'Date - First Published At (DS)|ID - (DS)|Incrementable Extracts (DS)|Is Hierarchical (DS)|'
    extract_header += 'Lock Version (DS)|LUID (DS)|Name (DS)|ID - Owner (DS)|ID - Project (DS)|Refreshable Extracts (DS)|'
    extract_header += 'ID - Rep Data (DS)|ID - Rep Extract Data (DS)|Repository URL (DS)|Revisions (DS)|ID - Site (DS)|'
    extract_header += 'Size (DS)|Sate (DS)|Table Name (DS)|Date - Updated At (DS)|Caption (Conn DS)|'
    extract_header += 'Date - Created At (Conn DS)|DB Class (Conn DS)|DB Name (Conn DS)|Has Extract (Conn DS)|'
    extract_header += 'ID - (Conn DS)|Keychain (Conn DS)|LUID (Conn DS)|Name (Conn DS)|ID - Owner (Conn DS)|'
    extract_header += 'Owner Type (Conn DS)|Password (Conn DS)|Port (Conn DS)|Server (Conn DS)|ID - Site (Conn DS)|'
    extract_header += 'Table Name (Conn DS)|Date - Updated At (Conn DS)|Username (Conn DS)|Date - Created At (Extract)|'
    extract_header += 'ID - Datasource (Extract)|Descriptor (Extract)|ID - (Extract)|Date - Updated At (Extract)|'
    extract_header += 'ID - Workbook (Extract)'

    # Append data to http_requests
    append_postgres_table(http_requests_rows,'http_requests.txt',parameters[13])
    append_postgres_table(user_logins_rows,'logins.txt',parameters[13])
    write_postgres_table(users_rows,'users.txt',parameters[13],users_header)
    write_postgres_table(views_rows,'views.txt',parameters[13],views_header)
    write_postgres_table(workbooks_rows,'workbooks.txt',parameters[13],workbooks_header)

    # If we need to copy files to shared drive do so
    if(len(parameters[12]) > 1):
        copyfile(parameters[11]+'users.txt',parameters[12]+'users.txt')
        copyfile(parameters[11]+'http_requests.txt',parameters[12]+'http_requests.txt')
        copyfile(parameters[11]+'logins.txt',parameters[12]+'logins.txt')
        copyfile(parameters[11]+'views.txt',parameters[12]+'views.txt')
        copyfile(parameters[11]+'workbooks.txt',parameters[12]+'workbooks.txt')

    extract_rows = execute_query_from_postgres_table(extract_conn,query_extract,parameters[9],parameters[10],True)
    write_postgres_table(extract_rows,'extracts.txt',parameters[13],extract_header)
    if(len(parameters[12]) > 1):
        copyfile(parameters[11]+'extracts.txt',parameters[12]+'extracts.txt')

    temp = []
    temp.append(nt(parameters[0],parameters[1],parameters[4],parameters[2],parameters[3],'multiple_tables'))
    temp.append(nt(parameters[0],parameters[1],parameters[5],parameters[2],parameters[3],'multiple_tables'))
    temp.append(nt(parameters[0],parameters[1],parameters[6],parameters[2],parameters[3],'multiple_tables'))
    temp.append(nt(parameters[0],parameters[1],parameters[7],parameters[2],parameters[3],'multiple_tables'))
    usage_wo_usage_conn = temp
    usage_wo_usage_query = 'SELECT * FROM ((('
    usage_wo_usage_query += 'SELECT D5.ID AS "A_WB_ID", D4.caption AS "Caption (Conn WB)",'
    usage_wo_usage_query += 'D4.created_at AS "Date - Created At (Conn WB)",D4.dbclass AS "DB Class (Conn WB)",'
    usage_wo_usage_query += 'D4.dbname AS "DB Name (Conn WB)",'
    usage_wo_usage_query += 'D4.has_extract AS "Has Extract (Conn WB)",'
    usage_wo_usage_query += 'D4.id AS "ID - (Conn WB)",'
    usage_wo_usage_query += 'D4.keychain AS "Keychain (Conn WB)",'
    usage_wo_usage_query += 'D4.luid AS "LUID (Conn WB)",D4.name AS "Name (Conn WB)",'
    usage_wo_usage_query += 'D4.owner_id AS "ID - Owner (Conn WB)",'
    usage_wo_usage_query += 'D4.owner_type AS "Owner Type (Conn WB)",'
    usage_wo_usage_query += 'D4.password AS "Password (Conn WB)",D4.port AS "Port (Conn WB)",'
    usage_wo_usage_query += 'D4.server AS "Server (Conn WB)",D4.site_id AS "ID - Site (Conn WB)",'
    usage_wo_usage_query += 'D4.tablename AS "Table Name (Conn WB)",D4.updated_at AS "Date - Updated At (Conn WB)",'
    usage_wo_usage_query += 'D4.username AS "Username (Conn WB)",D5.created_at AS "Date - Created At (WB DS)",'
    usage_wo_usage_query += 'D5.id AS "ID - (WB DS)",D5.name AS "Name (WB DS)",'
    usage_wo_usage_query += 'D5.owner_id AS "ID - Owner (WB DS)",D5.owner_name AS "Owner Name (WB DS)",'
    usage_wo_usage_query += 'D5.project_id AS "ID - Project (WB DS)",D5.project_name AS "Project Name (WB DS)",'
    usage_wo_usage_query += 'D5.site_id AS "ID - Site (WB DS)",D5.size AS "Size (WB DS)",'
    usage_wo_usage_query += 'D5.system_user_id AS "ID - System User (WB DS)",D5.updated_at AS "Date - Updated At (WB DS)",'
    usage_wo_usage_query += 'D5.view_count AS "View Count (WB DS)",D5.workbook_url AS "Workbook URL (WB DS)"'
    usage_wo_usage_query += ' FROM DATA_CONNECTIONS D4 INNER JOIN _WORKBOOKS D5 ON D5.ID = D4.OWNER_ID'
    usage_wo_usage_query += " WHERE D4.OWNER_TYPE = 'Workbook' ) AS A LEFT JOIN ( SELECT"
    usage_wo_usage_query += ' D2.asset_key_id AS "Asset Key (DS)",D2.connectable AS "Connectable (DS)",'
    usage_wo_usage_query += 'D2.content_version AS "Content Version (DS)",D2.created_at AS "Date - Created (DS)",'
    usage_wo_usage_query += 'D2.data_engine_extracts AS "Data Engine Extracts (DS)",D2.db_class AS "DB Class (DS)",'
    usage_wo_usage_query += 'D2.db_name AS "DB Name (DS)",D2.description AS "Description (DS)",'
    usage_wo_usage_query += 'D2.document_version AS "Document Version (DS)",D2.embedded AS "Embedded (DS)",'
    usage_wo_usage_query += 'D2.extracts_incremented_at AS "Date - Extracts Incremented At (DS)",D2.extracts_refreshed_at AS "Date - Extracts Refreshed At (DS)",'
    usage_wo_usage_query += 'D2.first_published_at AS "Date - First Published At (DS)",D2.id AS "ID - (DS)",'
    usage_wo_usage_query += 'D2.incrementable_extracts AS "Incrementable Extracts (DS)",D2.is_hierarchical AS "Is Hierarchical (DS)",'
    usage_wo_usage_query += 'D2.lock_version AS "Lock Version (DS)",D2.luid AS "LUID (DS)",'
    usage_wo_usage_query += 'D2.name AS "Name (DS)",D2.owner_id AS "ID - Owner (DS)",'
    usage_wo_usage_query += 'D2.project_id AS "ID - Project (DS)",D2.refreshable_extracts AS "Refreshable Extracts (DS)",'
    usage_wo_usage_query += 'D2.repository_data_id AS "ID - Rep Data (DS)",D2.repository_extract_data_id AS "ID - Rep Extract Data (DS)",'
    usage_wo_usage_query += 'D2.repository_url AS "Repository URL (DS)",D2.revision AS "Revisions (DS)",'
    usage_wo_usage_query += 'D2.site_id AS "ID - Site (DS)",D2.size AS "Size (DS)",'
    usage_wo_usage_query += 'D2.state AS "Sate (DS)",D2.table_name AS "Table Name (DS)",'
    usage_wo_usage_query += 'D2.updated_at AS "Date - Updated At (DS)",D4.caption AS "Caption (Conn DS)",'
    usage_wo_usage_query += 'D4.created_at AS "Date - Created At (Conn DS)",D4.dbclass AS "DB Class (Conn DS)",'
    usage_wo_usage_query += 'D4.dbname AS "DB Name (Conn DS)",D4.has_extract AS "Has Extract (Conn DS)",'
    usage_wo_usage_query += 'D4.id AS "ID - (Conn DS)",D4.keychain AS "Keychain (Conn DS)",'
    usage_wo_usage_query += 'D4.luid AS "LUID (Conn DS)",D4.name AS "Name (Conn DS)",'
    usage_wo_usage_query += 'D4.owner_id AS "ID - Owner (Conn DS)",D4.owner_type AS "Owner Type (Conn DS)",'
    usage_wo_usage_query += 'D4.password AS "Password (Conn DS)",D4.port AS "Port (Conn DS)",'
    usage_wo_usage_query += 'D4.server AS "Server (Conn DS)",D4.site_id AS "ID - Site (Conn DS)",'
    usage_wo_usage_query += 'D4.tablename AS "Table Name (Conn DS)",D4.updated_at AS "Date - Updated At (Conn DS)",'
    usage_wo_usage_query += 'D4.username AS "Username (Conn DS)" FROM DATASOURCES D2 INNER JOIN DATA_CONNECTIONS D4 ON D2.ID = D4.OWNER_ID'
    usage_wo_usage_query += " WHERE D4.OWNER_TYPE = 'Datasource' ) B"
    usage_wo_usage_query += ' ON A."DB Class (Conn WB)" = B."DB Class (Conn DS)" AND A."Name (Conn WB)" = B."Name (Conn DS)"'
    usage_wo_usage_query += ') C LEFT JOIN ( SELECT D1.created_at AS "Date - Created At (Extract)",'
    usage_wo_usage_query += 'D1.datasource_id AS "ID - Datasource (Extract)",D1.descriptor AS "Descriptor (Extract)",'
    usage_wo_usage_query += 'D1.id AS "ID - (Extract)",D1.updated_at AS "Date - Updated At (Extract)",'
    usage_wo_usage_query += 'D1.workbook_id AS "ID - Workbook (Extract)" FROM EXTRACTS D1'
    usage_wo_usage_query += ') D ON D."ID - Datasource (Extract)" = C."ID - (DS)" ) E'
    usage_wo_usage_query += ' LEFT JOIN ( SELECT D5.ID AS "F_WB_ID",D5.created_at AS "Date - Created At (WB)",'
    usage_wo_usage_query += 'D5.id AS "ID - (WB)",D5.name AS "Name (WB)",D5.owner_id AS "ID - Owner (WB)",'
    usage_wo_usage_query += 'D5.owner_name AS "Owner Name (WB)",'
    usage_wo_usage_query += 'D5.project_id AS "ID - Project (WB)",D5.project_name AS "Project Name (WB)",'
    usage_wo_usage_query += 'D5.site_id AS "ID - Site (WB)",D5.size AS "Size (WB)",'
    usage_wo_usage_query += 'D5.system_user_id AS "ID - System User (WB)",D5.updated_at AS "Date - Updated At (WB)",'
    usage_wo_usage_query += 'D5.view_count AS "View Count (WB)",D5.workbook_url AS "Workbook URL (WB)",'
    usage_wo_usage_query += 'D8.caption AS "Caption (Views)",'
    usage_wo_usage_query += 'D8.created_at AS "Date - Created At (Views)",D8.id AS "ID - (Views)",'
    usage_wo_usage_query += 'D8.index AS "Index (Views)",D8.name AS "Name (Views)",'
    usage_wo_usage_query += 'D8.owner_id AS "ID - Owner (Views)",D8.owner_name AS "Owner Name (Views)",'
    usage_wo_usage_query += 'D8.site_id AS "ID - Site (Views)",D8.title AS "Title (Views)",'
    usage_wo_usage_query += 'D8.view_url AS "View URL (Views)",D8.workbook_id AS "ID - Workbook (Views)",'
    usage_wo_usage_query += 'D9.domain_id AS "ID - Domain (WB Owner)",D9.domain_name AS "Domain Name (WB Owner)",'
    usage_wo_usage_query += 'D9.domain_short_name AS "Domain Short Name (WB Owner)",D9.friendly_name AS "Friendly Name (WB Owner)",'
    usage_wo_usage_query += 'D9.id AS "ID - (WB Owner)",D9.licensing_role_id AS "ID - Licensing Role (WB Owner)",'
    usage_wo_usage_query += 'D9.licensing_role_name AS "Licensing Role Name (WB Owner)",D9.login_at AS "Date - Login At (WB Owner)",'
    usage_wo_usage_query += 'D9.name AS "Name (WB Owner)",D9.site_id AS "ID - Site (WB Owner)",'
    usage_wo_usage_query += 'D9.system_user_id AS "ID - System User (WB Owner)" FROM _WORKBOOKS D5'
    usage_wo_usage_query += ' INNER JOIN _VIEWS D8 ON D8.WORKBOOK_ID = D5.ID'
    usage_wo_usage_query += ' INNER JOIN _users D9 ON D5.OWNER_ID = D9.ID ) F ON F."F_WB_ID" = E."A_WB_ID"'
    usage_wo_usage_header = 'A_WB_ID|Caption (Conn WB)|Date - Created At (Conn WB)|DB Class (Conn WB)|DB Name (Conn WB)|Has Extract (Conn WB)|'
    usage_wo_usage_header += 'ID - (Conn WB)|Keychain (Conn WB)|LUID (Conn WB)|Name (Conn WB)|ID - Owner (Conn WB)|Owner Type (Conn WB)|Password (Conn WB)|Port (Conn WB)|'
    usage_wo_usage_header += 'Server (Conn WB)|ID - Site (Conn WB)|Table Name (Conn WB)|Date - Updated At (Conn WB)|Username (Conn WB)|Date - Created At (WB DS)|'
    usage_wo_usage_header += 'ID - (WB DS)|Name (WB DS)|ID - Owner (WB DS)|Owner Name (WB DS)|ID - Project (WB DS)|Project Name (WB DS)|ID - Site (WB DS)|Size (WB DS)|'
    usage_wo_usage_header += 'ID - System User (WB DS)|Date - Updated At (WB DS)|View Count (WB DS)|Workbook URL (WB DS)|Asset Key (DS)|Connectable (DS)|Content Version (DS)|'
    usage_wo_usage_header += 'Date - Created (DS)|Data Engine Extracts (DS)|DB Class (DS)|DB Name (DS)|Description (DS)|Document Version (DS)|Embedded (DS)|'
    usage_wo_usage_header += 'Date - Extracts Incremented At (DS)|Date - Extracts Refreshed At (DS)|Date - First Published At (DS)|ID - (DS)|Incrementable Extracts (DS)|'
    usage_wo_usage_header += 'Is Hierarchical (DS)|Lock Version (DS)|LUID (DS)|Name (DS)|ID - Owner (DS)|ID - Project (DS)|Refreshable Extracts (DS)|ID - Rep Data (DS)|'
    usage_wo_usage_header += 'ID - Rep Extract Data (DS)|Repository URL (DS)|Revisions (DS)|ID - Site (DS)|Size (DS)|Sate (DS)|Table Name (DS)|Date - Updated At (DS)|'
    usage_wo_usage_header += 'Caption (Conn DS)|Date - Created At (Conn DS)|DB Class (Conn DS)|DB Name (Conn DS)|Has Extract (Conn DS)|ID - (Conn DS)|Keychain (Conn DS)|'
    usage_wo_usage_header += 'LUID (Conn DS)|Name (Conn DS)|ID - Owner (Conn DS)|Owner Type (Conn DS)|Password (Conn DS)|Port (Conn DS)|Server (Conn DS)|'
    usage_wo_usage_header += 'ID - Site (Conn DS)|Table Name (Conn DS)|Date - Updated At (Conn DS)|Username (Conn DS)|Date - Created At (Extract)|ID - Datasource (Extract)|'
    usage_wo_usage_header += 'Descriptor (Extract)|ID - (Extract)|Date - Updated At (Extract)|ID - Workbook (Extract)|F_WB_ID|Date - Created At (WB)|ID - (WB)|'
    usage_wo_usage_header += 'Name (WB)|ID - Owner (WB)|Owner Name (WB)|ID - Project (WB)|Project Name (WB)|ID - Site (WB)|Size (WB)|ID - System User (WB)|'
    usage_wo_usage_header += 'Date - Updated At (WB)|View Count (WB)|Workbook URL (WB)|Caption (Views)|Date - Created At (Views)|ID - (Views)|Index (Views)|'
    usage_wo_usage_header += 'Name (Views)|ID - Owner (Views)|Owner Name (Views)|ID - Site (Views)|Title (Views)|View URL (Views)|ID - Workbook (Views)|'
    usage_wo_usage_header += 'ID - Domain (WB Owner)|Domain Name (WB Owner)|Domain Short Name (WB Owner)|Friendly Name (WB Owner)|ID - (WB Owner)|'
    usage_wo_usage_header += 'ID - Licensing Role (WB Owner)|Licensing Role Name (WB Owner)|Date - Login At (WB Owner)|Name (WB Owner)|'
    usage_wo_usage_header += 'ID - Site (WB Owner)|ID - System User (WB Owner)'
    usage_wo_usage_rows = execute_query_from_postgres_table(usage_wo_usage_conn,usage_wo_usage_query,parameters[9],parameters[10],True)
    write_postgres_table(usage_wo_usage_rows,'Extracts_To_Workbooks.txt',parameters[13],usage_wo_usage_header)
    
    # If we need to copy files to shared drive do so
    if(len(parameters[12]) > 1):
        copyfile(parameters[11]+'Extracts_To_Workbooks.txt',parameters[12]+'Extracts_To_Workbooks.txt')

run()
