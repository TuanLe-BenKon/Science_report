from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
drive = GoogleDrive(gauth)


def upload_file(pdf_dir: str, filename: str, folder_id: str) -> None:

    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

    try:
        for f in file_list:
            if f['title'] == filename:
                f.Delete()
    except:
        pass

    f = drive.CreateFile({'parents': [{'id': folder_id}], 'title': filename})
    f.SetContentFile(pdf_dir)
    f.Upload(param={'supportsTeamDrives': True})

