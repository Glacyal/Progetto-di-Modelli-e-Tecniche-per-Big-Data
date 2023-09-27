import os
import shutil

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from datetime import datetime

def google_auth():
    gauth = GoogleAuth()
    # use local default browser for authentication

    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    return gauth, drive

def upload_backup(drive, path, file_name):
    # create a google drive file instance with title metadata
    f = drive.CreateFile({'title': file_name})
    # set the path to zip file
    f.SetContentFile(os.path.join(path, file_name))
    # start upload
    f.Upload()
    # set f to none because of a vulnerability found in PyDrive
    f = None

def create_zip(path, file_name):
    # use shutil to create a zip file
    try:
        shutil.make_archive(f"backup/{file_name}", 'zip', path)
        return True
    except FileNotFoundError as e:
        return False

def controller():
    # folder path to backup
    path = "states"
    # get machine date and time
    now = datetime.now()
    # new backup name
    file_name = "backup " + now.strftime('%H-%M %d-%m-%Y').replace('/', '-')
    print(file_name)
    # if zip creation fails then abort execution
    if create_zip(path, file_name):
        print()
        # start API authentication
        auth, drive = google_auth()
        # start file upload
        upload_backup(drive, "backup", file_name + '.zip')

if __name__ == '__main__':

    controller()

