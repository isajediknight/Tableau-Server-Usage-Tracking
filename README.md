# Tableau Server Usage Tracking
## Description
Simple Python Program to extract tables from the Tableau Server Postgres DB for the purpose of saving off the _http_requests table which is truncated every Tableau Server Backup.  The contents of these tables are saved as text files and copied to shared drive.  Extracts can then be created off of the flat files in the shared drive so the flat files are automatically updated.  The Extracts placed on a shared drive Tableau Server has access to can be set to be automatically refreshed daily.  Once your script is in place no manual steps are required for this data to be updated.
## Installation
1. Install Python 3.2.2 64 bit: python-3.2.2.amd64.msi https://www.python.org/download/releases/3.2.2/
2. Install psycopg2-2.6.1.win-amd64-py3.2-pg9.4.4-release.exe (I could no longer find the site where I originally downloaded this file - added to repo in Helpful_Installation_Files)
3. Add Python to your path variable (Add_Python_to_Env_Var_Path.png)
4. Update "input_files\parameters.txt" with the values for your Tableau Server.

## Optional Installation
Create a .bat file which is run by Windows Scheduler every day at 12:02am.  This will capture all the previous days Usage Tracking data.

## Create Main Extract: Tableau_Server_Master_Usage_Tracking
I renamed several fields.  You will have to correct it them if you use the Workbooks included here.

Use the below Images as examples:

<img src="https://github.com/isajediknight/Tableau-Server-Usage-Tracking/blob/master/Helpful_Installation_Files/Tableau_Server_Master_Usage_Tracking_Extract_1.png" />
<img src="https://github.com/isajediknight/Tableau-Server-Usage-Tracking/blob/master/Helpful_Installation_Files/Tableau_Server_Master_Usage_Tracking_Extract_2.png" />
<img src="https://github.com/isajediknight/Tableau-Server-Usage-Tracking/blob/master/Helpful_Installation_Files/Tableau_Server_Master_Usage_Tracking_Extract_3.png" />

Extracts_To_Workbooks.txt joins to http_requests.txt with a left join on: View URL(Views) = Current Sheet (http)

http_requests.txt joins to users.txt with an inner join on: ID - User (http) = ID - (http User)

### Name and Field changes for the Extract

Add: ``` (http User)``` to the end of every field under users.txt

Create: ```Friendly Name Count Distinct (http User)``` with calculation: ```COUNTD([Friendly Name (http User)])```

Create calculated field named: ```Date - Date Completed At (http)``` with calculation: ```DATE([Date - Completed At (http)])```

## File Explanations
### Input Files
#### parameters.txt
```
{Description of line}Information needed
```

```
{postgres database}workgroup
{postgres username}readonly
{postgres password}PASSWORD
{postgres port}8060
{Tableau Server Worker 1}SERVERNAME
{Tableau Server Worker 2}SERVERNAME
{Tableau Server Worker 3}SERVERNAME
{Tableau Server Worker 4}SERVERNAME
{GMT or UTC hour difference}6
{Connection attempts}10
{Seconds to wait to retry}5
{Local File Location}C:/LOCATION/OF/YOUR/REPO/
{Shared Drive File Location}//SHARED/DRIVE/LOCATION/
{Delimiter}|
```

### Output Files
#### extracts.txt
List of all Extracts

#### Extracts_To_Workbooks.txt
Combines multiple tables into one result file.  This is where Extracts and joined to Workbooks.

#### http_requests.txt
This table is appeneded to each time it is run.  If the code is run twice in the same day you will get duplicate data.

#### logins.txt
Contains login data from previous day.  Note: Only the most recent action is recorded by time here.  Tableau Server only tracks the last time an action happened in this table - thus you will not see when someone logged in you will only see the time of the last action they performed.

#### users.txt
List of all users on Tableau Server.  This is used twice in Tableau_Server_Master_Usage_Tracking.  Once for Workbook owners and once for http_requests users.

#### views.txt
List of all Views on Tableau Server.

#### workbooks.txt
List of all Workbooks on Tableau Server.

## Usage
This code is designed to be run once day.  Ideally at 12:02am local time.  The program can be executed by running the following command from Command Prompt in your Python directory or via a .bat file:
```
python Export_Tableau_Server_Usage_Tracking.py
```

## Assumptions
* You have already created the readonly user for your Tableau Server Postgres DB: http://onlinehelp.tableau.com/current/server/en-us/help.htm#adminview_postgres_access.htm

## Versions of Software Used

Windows 7 64 bit

Python 3.2.2

psycopg 2-2.6.1

Tableau Desktop 9.3

Tableau Server 9.3

## Improvements
This code was originally developed as a "poor man" solution.  Quick and easy to save off this data.  A more Enterprise approach would be to bring this data in a Data Warehouse using Informatica/ODI/etc and load the data into DB Tables instead of flat files.  Then base the Extract off of the table in the DB instead of flat files.

## Warnings
If you have thousands of users hitting Tableau Server everyday the http_requests.txt will get very big very fast and could start filling up your drive.

The header for http_requests had to be hard coded since the file is appended.  If the order of the fields ever change it will cause an issue in the Extract.

## History
02/29/2016 Postgres query written and shared to: https://community.tableau.com/thread/201905

03/18/2016 Submission made for Talk at Tableau Conference 2016

07/13/2016 Talk submission denied for Tableau Conference 2016

07/30/2016 Github Repository created for sharing code

08/01/2016 Code posted to Tableau Community Forum

## Credits
Entity Relationship Diagram used from: http://onlinehelp.tableau.com/current/server/en-us/help.htm#data_dictionary.html

Help was requested from Tableau Support: Case 01918537.  I was told that querying the Tableau Server Postgres DB was only permitted but not officially supported and that they would not help tweak this usage tracking query.  Any improvements to this code and query behind it are welcome.

## License
Copyright (C) 02/29/2016, Luke Brady, Cerner Corporation. This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
