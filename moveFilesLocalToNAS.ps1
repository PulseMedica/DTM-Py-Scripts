# moveFilesLocalToNAS.ps1 -- written 3/13/2023 by Jeff Strom
# intended to run as a Windows Scheduled Task or Linux cron job.
# must be redirected to target folders
#================================================
Invoke-Expression 'python moveFilesLocalToNAS.py "C:/Users/jeff/source/repos/DTM-Py-Scripts/Data" "C:/Users/jeff/source/repos/DTM-Py-Scripts"'
