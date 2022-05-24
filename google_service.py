from google.oauth2 import service_account
from googleapiclient import discovery
import os

def get_service(json_path):
    scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file']
    jsonfile = json_path
    credentials = service_account.Credentials.from_service_account_file(jsonfile, scopes = scopes)
    service = discovery.build('sheets','v4',credentials = credentials)
    return service

