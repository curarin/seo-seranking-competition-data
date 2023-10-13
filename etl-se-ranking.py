import pandas as pd
import xlrd
import numpy as np
import io
import os 
from pandas.io import gbq
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import dateutil.relativedelta
import datetime
print(np.__version__)
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
# Variablen Section - Anpassen je nach Kunde / Domain
bq_table = "###"
recipients = ["###", "seo@paulherzog.at"]
mail_content = "Aktuelle Daten sind im Dashboard vorhanden. Bitte folge dem Link: ###"

# Außerdem noch anzupassen: FileID unter "file_list"
def login_now():
    """
    Google Drive service with a service account.
    note: for the service account to work, you need to share the folder or
    files with the service account email.

    :return: google auth
    """
    # Define the settings dict to use a service account
    # We also can use all options available for the settings dict like
    # oauth_scope,save_credentials,etc.
    settings = {
                "client_config_backend": "service",
                "service_config": {
                    "client_json_file_path": "/key.json",
                }
            }
    # Create instance of GoogleAuth
    gauth = GoogleAuth(settings=settings)
    # Authenticate
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    destination_file = "/tmp/se_ranking.xls"
    source_file = "se_ranking.xls"
    # List files in Google Drive - q ist hier der Job Traveler Folder
    file_list = drive.ListFile({'q': "'1QamY00VYE8Vrl5wtww7UjmFa2G64A8OP' in parents and trashed=false"}).GetList()

    # Find file to update
    for file1 in file_list:
        print('title: %s, id: %s' % (file1['title'], file1['id']))
        if file1['title'] == source_file:
            file_of_interest = file1

    file_of_interest.GetContentFile(destination_file)
    file_of_interest["title"] = "se_ranking_old.xls"
    file_of_interest.Upload()
    
    workbook = xlrd.open_workbook(destination_file, ignore_workbook_corruption=True)
    se_ranking_raw = pd.read_excel(workbook)
    se_ranking_raw.columns = se_ranking_raw.iloc[4]
    se_ranking_raw = se_ranking_raw.iloc[5:]
    se_ranking_raw = se_ranking_raw.loc[:, se_ranking_raw.columns.notna()]

    # Tagging based on SE Ranking Tags
    se_ranking_raw["Tag"] = np.where(se_ranking_raw["Search Vol."].isna(), se_ranking_raw["Keyword"], "NaN")
    se_ranking_raw = se_ranking_raw.replace("NaN", np.nan)
    se_ranking_raw["Tag"] = se_ranking_raw.Tag.replace(r'^\s*$', "NaN", regex=True).ffill()
    se_ranking_raw = se_ranking_raw[se_ranking_raw["Search Vol."].notna()]

    # Melting Data
    se_ranking_raw = se_ranking_raw.melt(["Keyword", "Search Vol.", "Tag"], var_name="Domains", value_name="Ranking")
    se_ranking_raw = se_ranking_raw.replace(["-", "ND"], 0)

    # Tagging nach Google Seite
    se_ranking_raw["Google_Seite"] = pd.cut(se_ranking_raw["Ranking"], bins=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100], labels=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
    se_ranking_raw = se_ranking_raw.replace("NaN", np.nan)
    se_ranking_raw["Google_Seite"] = se_ranking_raw["Google_Seite"].astype(str)
    se_ranking_raw = se_ranking_raw.replace("nan", "0")
    se_ranking_raw["Google_Seite"] = se_ranking_raw["Google_Seite"].astype(int)

    # date today
    #now = datetime.datetime.now()
    #prev = now + dateutil.relativedelta.relativedelta(months=-1)
    #se_ranking_raw["Date"] = prev.strftime("%Y-%m-%d")
    se_ranking_raw["Date"] = "2023-01-01"
    se_ranking_raw["Date"] = se_ranking_raw["Date"].apply(pd.to_datetime)

    # string replace http / https
    se_ranking_raw = se_ranking_raw.replace(to_replace = 'https?://', value = '', regex = True)
    
    # rename invalid column names for bigquery
    se_ranking_raw = se_ranking_raw.rename(columns={"Search Vol.": "SV"})
    
    # traffic forecast
    sistrix_ctr = [["1", 0.342], ["2", 0.171], ["3", 0.114], ["4", 0.081], ["5", 0.074], ["6", 0.051], ["7", 0.041], ["8", 0.033], ["9", 0.029], ["10", 0.026]]
    df_sistrix_ctr = pd.DataFrame(sistrix_ctr, columns=["Ranking", "CTR"])
    df_sistrix_ctr["Ranking"] = df_sistrix_ctr["Ranking"].astype(int)
    se_ranking_raw = se_ranking_raw.merge(df_sistrix_ctr, how="left", on="Ranking")
    se_ranking_raw["traffic_forecast"] = se_ranking_raw["SV"] * se_ranking_raw["CTR"]
    se_ranking_raw["traffic_forecast"] = se_ranking_raw["traffic_forecast"].round(decimals=0)
    
    return se_ranking_raw
    

se_ranking = login_now()

credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )
pandas_gbq.to_gbq(se_ranking, bq_table, project_id="###", if_exists="append", credentials=credentials)

    
import smtplib, ssl
from email.message import EmailMessage

port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "seo@paulherzog.at"  
password = "###"

msg = EmailMessage()
msg.set_content(mail_content)
msg['Subject'] = "SEO Daten Update | Reporting"
msg['From'] = sender_email
msg['To'] = recipients

context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.send_message(msg, from_addr=sender_email, to_addrs=recipients)
