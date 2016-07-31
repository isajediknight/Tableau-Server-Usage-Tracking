# Tableau Server Usage Tracking
## Description
Simple Python Program to extract tables from the Tableau Server Postgres DB for the purpose of saving off the _http_requests table which is truncated every Tableau Server Backup.  The contents of these tables are saved as text files and copied to shared drive.  Extracts can then be created off of the flat files in the shared drive so the flat files are automatically updated.  The Extracts placed on a shared drive Tableau Server has access to can be set to be automatically refreshed daily.  Once your script is in place no manual steps are required for this data to be updated.
## Installation
1. Install Python 3.2.2 64 bit: python-3.2.2.amd64.msi https://www.python.org/download/releases/3.2.2/
2. Install psycopg2-2.6.1.win-amd64-py3.2-pg9.4.4-release.exe
3. Place all the files into your "C:\Python32" folder as shown in "Placement_Of_Files.png"
4. Update "C:\Python32\input_files\parameters.txt" with the values for your Tableau Server.

## Usage
This code is designed to be run once day.  Ideally at 12:02am local time.  The program can be executed by running the following command from Command Prompt in your Python directory:
python Export_Tableau_Server_Usage_Tracking.py

## Assumptions
* You have already created the readonly user for your Tableau Server Postgres DB: http://onlinehelp.tableau.com/current/server/en-us/help.htm#adminview_postgres_access.htm
* You know 

## File Explanations
### Input Files
### Output Files
extracts.txt

Extracts_To_Workbooks.txt

http_requests.txt

logins.txt

users.txt

views.txt

workbooks.txt

## Versions of Software Used
Windows 7 64 bit
Python 3.2.2
psycopg 2-2.6.1
Tableau Server 9.3

## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D
## History
02/29/2016 Postgres query written and shared to: https://community.tableau.com/thread/201905
03/18/2016 Submission made for Talk at Tableau Conference 2016
07/13/2016 Talk submission denied for Tableau Conference 2016
07/30/2016 Github Repository created for sharing code
## Credits
Entity Relationship Diagram used from: http://onlinehelp.tableau.com/current/server/en-us/help.htm#data_dictionary.html

## License
Copyright (C) 02/29/2016, Luke Brady, Cerner Corporation. This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
