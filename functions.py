from datetime import datetime, timezone
from pymongo import MongoClient
import re
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()
mongo_pat = os.getenv('mongo_pat')
api_pipedrive = os.getenv('api_pipedrive')

client = MongoClient( f'mongodb+srv://{mongo_pat}',tls=True,tlsAllowInvalidCertificates=True)

def get_conso():
    print("########### GET CONSO START ###########")

    result = client['legacy-api-management']['bills'].aggregate([
        {
            '$match': {
                'type': 'resume',
                'createdAt': {
                    '$gt': datetime(2022, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
                }
            }
        }, {
            '$addFields': {
                'societyObjectId': {
                    '$toObjectId': '$societyId'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'societyId': '$societyObjectId',
                    'yearMonth': {
                        '$dateToString': {
                            'format': '%Y-%m',
                            'date': '$createdAt'
                        }
                    }
                },
                'totalAmount': {
                    '$sum': '$price.amount'
                }
            }
        }, {
            '$lookup': {
                'from': 'societies',
                'localField': '_id.societyId',
                'foreignField': '_id',
                'as': 'society'
            }
        }, {
            '$unwind': '$society'
        }, {
            '$project': {
                'totalAmount': 1,
                'societyName': '$society.name',
                '_id.societyId': 1,
                '_id.yearMonth': 1
            }
        }, {
            '$sort': {
                '_id.yearMonth': 1
            }
        }
    ])

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(result)
    df['societyName'] = df['societyName'].str.replace('+Simple', 'Plus Simple')

    # If the '_id' column exists, process it
    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: x if isinstance(x, dict) else {})
        df = df.join(pd.json_normalize(df['_id'])).drop(columns=['_id'])

    # Convert 'yearMonth' to datetime format
    df['yearMonth'] = pd.to_datetime(df['yearMonth'], format='%Y-%m')

    # Define months for expansion
    months = [
        "octobre", "novembre", "décembre", "janvier", "février", "mars", "avril",
        "mai", "juin", "juillet", "août", "septembre"
    ]

    # Extract unique societies
    societies = df[['societyName', 'societyId']].drop_duplicates()

    # Create a new dataframe where each society will have a row for each month
    new_data = pd.DataFrame([(society, society_id, month)
                             for society, society_id in zip(societies['societyName'], societies['societyId'])
                             for month in months],
                            columns=['societyName', 'societyId', 'mois'])

    # Step 2: Convert month names to numerical values for merging with the original data
    month_mapping = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
        "juillet": 7, "août": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
    }

    new_data['mois_num'] = new_data['mois'].map(month_mapping)

    # Extract year and month from yearMonth column in the original dataframe
    df['year'] = df['yearMonth'].dt.year
    df['month'] = df['yearMonth'].dt.month

    df['yearMonth'] = pd.to_datetime(df['yearMonth'], format='%Y-%m')
    df.to_csv('csv/warning.csv')

    # Now perform the merge including 'yearMonth' explicitly
    df = new_data.merge(df[['societyName', 'societyId', 'year', 'month', 'totalAmount', 'yearMonth']],
                        how='left',
                        left_on=['societyName', 'societyId', 'mois_num'],
                        right_on=['societyName', 'societyId', 'month'])

    # Now apply the year range logic
    ranges = {
        "2022-2023": ("2022-10-01", "2023-09-30"),
        "2023-2024": ("2023-10-01", "2024-09-30"),
        "2024-2025": ("2024-10-01", "2025-09-30")
    }

    # Creating the new columns based on the defined ranges
    for new_col, (start, end) in ranges.items():
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        df[new_col] = df.apply(lambda row: row['totalAmount'] if start_date <= row['yearMonth'] <= end_date else None,
                               axis=1)


    # Continue with renaming months and sorting
    month_rename_mapping = {
        "octobre": "01-octobre",
        "novembre": "02-novembre",
        "décembre": "03-décembre",
        "janvier": "04-janvier",
        "février": "05-février",
        "mars": "06-mars",
        "avril": "07-avril",
        "mai": "08-mai",
        "juin": "09-juin",
        "juillet": "10-juillet",
        "août": "11-août",
        "septembre": "12-septembre"
    }

    # Apply the month renaming
    df['mois'] = df['mois'].replace(month_rename_mapping)

    # Convert 'yearMonth' column to datetime for easier manipulation
    df['yearMonth'] = pd.to_datetime(df['yearMonth'], format='%Y-%m')

    # Sort the DataFrame by societyId and yearMonth for consecutive month check
    df = df.sort_values(by=['societyId', 'yearMonth'])

    # Group the data by societyId to analyze consumption for each client
    for societyId, group in df.groupby('societyId'):
        group = group.sort_values(by='yearMonth')  # Ensure it's sorted by time

        # Check for three consecutive months with zero or NaN in totalAmount
        for i in range(len(group) - 2):
            if group['totalAmount'].iloc[i:i + 3].fillna(0).sum() == 0:
                df.loc[group.index[i:i + 3], 'warning'] = 3

        # Check for two consecutive months with zero or NaN in totalAmount
        for i in range(len(group) - 1):
            if group['totalAmount'].iloc[i:i + 2].fillna(0).sum() == 0 and df.loc[group.index[i], 'warning'] == "":
                df.loc[group.index[i:i + 2], 'warning'] = 2

    def assign_range(row):
        year_month = row['yearMonth']
        for range_name, (start, end) in ranges.items():
            start_date = pd.to_datetime(start)
            end_date = pd.to_datetime(end)
            if start_date <= year_month <= end_date:
                return range_name
        return None

    # Apply the function to create a new column 'range'
    df['range'] = df.apply(assign_range, axis=1)

    # Sort by societyName and then by mois
    sorted_data = df.sort_values(by=['societyName', 'mois'])

    sorted_data = sorted_data.drop(columns=['mois_num', 'year', 'month','totalAmount'], errors='ignore')
    sorted_data['yearMonth'] = sorted_data['yearMonth'].dt.strftime('%Y-%m')
    sorted_data['mois'] = sorted_data['mois'].astype(str)

    # Save the final sorted data to Excel
    sorted_data.to_csv('csv/conso.csv', index=False)


def warning():
    data = pd.read_csv("csv/warning.csv")

    # Step 1: Ensure yearMonth is formatted as "YYYY-MM"
    data['yearMonth'] = pd.to_datetime(data['yearMonth']).dt.strftime('%Y-%m')

    # Step 2: Create a new column "warning_month" initialized to None
    data['warning_month'] = None

    # Step 3: Create a range of months for each societyId starting from Oct 2022 to the current month
    min_month = pd.Timestamp.today() - pd.DateOffset(months=6)
    current_month = pd.Timestamp.today().replace(day=1)

    # Create a range of months for the last 6 months
    all_months = pd.date_range(min_month, current_month, freq='MS').strftime('%Y-%m')

    # Function to fill missing months with totalAmount = 0
    def fill_missing_months(group):
        # Get the existing months for the society
        existing_months = group['yearMonth'].unique()
        missing_months = [month for month in all_months if month not in existing_months]

        # Create rows for missing months
        missing_rows = pd.DataFrame({
            'yearMonth': missing_months,
            'totalAmount': 0,
            'societyId': group['societyId'].iloc[0],
            'societyName': group['societyName'].iloc[0],
            'warning_month': None
        })

        # Append the missing rows and return
        return pd.concat([group, missing_rows], ignore_index=True)

    # Apply the function to fill missing months for each societyId
    data_filled = data.groupby('societyId').apply(fill_missing_months).reset_index(drop=True)

    # Step 4: Check for consecutive months with totalAmount = 0 only in the last 6 months
    def set_warning_month(group):
        # Sort the group by yearMonth and focus on the last 6 months
        group = group[group['yearMonth'].isin(all_months)].sort_values(by='yearMonth')

        # Calculate consecutive 0s in totalAmount, starting from the current month backwards
        zero_streak = (group['totalAmount'] == 0).astype(int).groupby(group['totalAmount'].ne(0).cumsum()).cumsum()

        # Set warning_month for the current month based on the zero_streak
        current_row_index = group[group['yearMonth'] == current_month.strftime('%Y-%m')].index
        if not current_row_index.empty:
            streak_value = zero_streak.loc[current_row_index].values[0]
            group.loc[current_row_index, 'warning_month'] = streak_value if streak_value >= 2 else None

        return group

    # Apply the warning logic for each societyId
    data_final = data_filled.groupby('societyId').apply(set_warning_month).reset_index(drop=True)

    # Keep only the relevant columns
    columns_to_keep = ['totalAmount', 'societyName', 'societyId', 'yearMonth', 'warning_month']
    final_data = data_final[columns_to_keep]

    # Step 5: Filter results and keep only one societyName if warning is present
    def filter_max_warning(group):
        # Keep only rows where warning_month is not None
        group_with_warning = group.dropna(subset=['warning_month'])

        # If there are any rows with a warning, select the one with the highest warning_month
        if not group_with_warning.empty:
            max_warning_row = group_with_warning.loc[group_with_warning['warning_month'].idxmax()]
            return pd.DataFrame([max_warning_row])
        else:
            # If no warnings are present, return nothing (empty DataFrame)
            return pd.DataFrame()

    # Apply the filter to keep only the max warning per society
    filtered_data = data_final.groupby('societyId').apply(filter_max_warning).reset_index(drop=True)

    # Save the final filtered data to a CSV file
    filtered_data.to_csv('csv/warning.csv', index=False)

def get_base():
    print("########### GET BASE START ###########")
    from pymongo import MongoClient

    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$match': {
                    'status': 0
                }
            }, {
            '$project': {
                'id': 1,
                'name': 1,
                'status': 1,
                'sub_price': 1,
                'fceCode': 1,
                'createdAt': 1,
                'salesName': 1,
                'fullVoucher':1,
                'pack_name': {'$arrayElemAt': ["$settings.subscriptions.name", 0]},
                'pack_price': {'$arrayElemAt': ["$settings.subscriptions.price.amount", 0]},
                'bluebizz': '$settings.flight.bluebizz',
                'ssoConnect': '$settings.config.ssoConnect'
            }
        }, {
            '$lookup': {
                'from': 'items',
                'localField': 'id',
                'foreignField': 'society._id',
                'as': 'items'
            }
        }, {
            '$project': {
                'id': 1,
                'name': 1,
                'status': 1,
                'sub_price': 1,
                "pack_name":1,
                "pack_price":1,
                 'fceCode': 1,
                'createdAt': 1,
                'bluebizz': 1,
                'ssoConnect': 1,
                'salesName':1,
                'fullVoucher': 1,

            }
        }, {
            '$sort': {
                'createdAt': -1
            }
        }
        ])

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(result)

    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)
    df.to_csv('csv/base.csv', index=False)

def get_tarif():
    print("########### GET TARIF START ###########")

    # Aggregation pipeline
    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$match': {
                    'status': 0,
                    'name': {
                        '$not': re.compile(r"(?i)newrest")
                    }
                }
            }, {
                '$project': {
                    'name': 1,
                    'hotel': '$settings.hotel.SAB',
                    'flights': '$settings.flight.sabre.vendor',
                    'corporateCodes' : '$corporateCodes',
                }
            }
        ]
    )

    # Convert result to DataFrame
    df = pd.DataFrame(list(result))

    # Replace '+Simple' with 'Plus Simple' in 'name' column
    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)

    # Save DataFrame to CSV
    df.to_csv('csv/tarif.csv', index=False)

def get_entities():
    print("########### GET ENTITIES START ###########")

    # Agrégation initiale pour extraire les entités
    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$unwind': '$billings'
            }, {
            '$project': {
                'name': 1,
                '_id': 1,
                "raison": "$billings.raison",
                "address": "$billings.address.label",
                "service": "$billings.service",
                "mandatId": "$billings.mandatId",
                "mandat_status": "$billings.status",
                "amex": "$settings.amex.cardHolderName",
                "billing_id": "$billings._id"  # Ajout de billings._id pour la recherche ultérieure
            }
        }
        ]
    )

    # Convertir le résultat en DataFrame
    df = pd.DataFrame(list(result))

    # Remplacer '+Simple' par 'Plus Simple' dans la colonne 'name'
    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)

    # Fonction pour créer la colonne "payment"
    def remplir_payment(row):
        if pd.notna(row['mandatId']) and pd.isna(row['amex']):
            return "mandat"
        elif pd.notna(row['amex']):
            return "carte logée"
        else:
            return "virement"

    # Appliquer la fonction à chaque ligne pour la colonne "payment"
    df['payment'] = df.apply(remplir_payment, axis=1)

    # Supprimer la colonne 'billing_id' si elle n'est plus nécessaire
    df.drop(columns=['billing_id'], inplace=True)

    # Sauvegarder le DataFrame en CSV
    df.to_csv('csv/entities.csv', index=False)

    print("CSV saved successfully.")

def get_entities_unactive():
    print("########### GET UPDATEDRIVE START ###########")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import pandas as pd
    from pymongo import MongoClient
    from bson import ObjectId

    # Pipeline d'agrégation pour récupérer les données de MongoDB
    pipeline = [
        {
            '$match': {
                'status': 'unactive'
            }
        },
        {
            '$addFields': {
                'companyIdAsObjectId': {'$toObjectId': '$companyId'}
            }
        },
        {
            '$lookup': {
                'from': 'societies',
                'localField': 'companyIdAsObjectId',
                'foreignField': '_id',
                'as': 'company_info'
            }
        },
        {
            '$unwind': '$company_info'
        },
        {
            '$project': {
                '_id': 1,  # Inclure _id
                'name': '$company_info.name',  # Inclure name de la collection societies
                'raison': 1,  # Inclure raison
                'address': '$address.label',  # Inclure adresse
                'status': 1,


                # Inclure status


            }
        }
    ]

    # Exécution de l'agrégation
    results = list(client['legacy-api-management']['billings'].aggregate(pipeline))

    # Transformation des résultats en DataFrame pandas
    df = pd.DataFrame(results)

    # Conversion des ObjectId en chaînes de caractères
    df['_id'] = df['_id'].astype(str)

    # Réorganisation des colonnes dans l'ordre souhaité
    df = df[['_id', 'name', 'raison', 'address', 'status']]

    # ID de votre feuille Google Sheets
    SPREADSHEET_ID = '1TI28QrhQ63i2bYgbOj4QFdriecANSalxXCZx3LEPCr8'

    # Onglet pour les entités inactives
    SHEET_NAME_UNACTIVE = 'Entities_Unactive'

    # Onglet pour les entités actives (nouvel onglet)
    SHEET_NAME_ACTIVE = 'Entities_Active'

    # Authentification Google Sheets
    SERVICE_ACCOUNT_FILE = 'creds/n8n-api-311609-115ae3a49fd9.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    # Convertir le DataFrame des entités inactives en une liste de listes
    values_unactive = df.values.tolist()
    values_unactive.insert(0, df.columns.tolist())  # Ajouter les noms de colonnes en haut

    # Préparation des données pour les entités inactives
    body_unactive = {
        'values': values_unactive
    }

    # Mise à jour de l'onglet des entités inactives
    result_unactive = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME_UNACTIVE,
        valueInputOption='RAW',
        body=body_unactive
    ).execute()

    print(f"{result_unactive.get('updatedCells')} cellules mises à jour dans '{SHEET_NAME_UNACTIVE}'.")

    # Charger le fichier CSV entities.csv dans un DataFrame
    df_entity = pd.read_csv('csv/entities.csv')

    # Nettoyer les données en remplaçant les NaN par une chaîne vide
    df_entity = df_entity.fillna('')

    # Convertir le DataFrame en une liste de listes
    values_active = df_entity.values.tolist()
    values_active.insert(0, df_entity.columns.tolist())  # Ajouter les noms de colonnes en haut

    # Préparation des données pour les entités actives
    body_active = {
        'values': values_active
    }

    # Mise à jour de l'onglet pour les entités actives
    result_active = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME_ACTIVE,  # Écrire dans un autre onglet
        valueInputOption='RAW',
        body=body_active
    ).execute()

    print(f"{result_active.get('updatedCells')} cellules mises à jour dans '{SHEET_NAME_ACTIVE}'.")

def get_portefeuille():
    print("########### GET PORTEFEUILLE START ###########")

    FILTER_ID = 1514
    url = f"https://api.pipedrive.com/v1/organizations?filter_id={FILTER_ID}&limit=500&api_token={api_pipedrive}"

    headers = {'Accept': 'application/json'}

    response = requests.get(url, headers=headers).json()['data']

    status_mapping = {
        "763": "ACTIF", "755": "ACTIF", "746": "ACTIF",
        "747": "INACTIF", "749": "TEST", "750": "TEST",
        "748": "INACTIF", "751": "INACTIF"
    }

    data = []
    for org in response:

        society_id = org['9d0760fac9b60ea2d3f590d3146d758735f2896d']
        society_name = org['name']
        actif = status_mapping.get(org['a056613671b057f83980e4fd4bb6003ce511ca3d'],
                                     org['a056613671b057f83980e4fd4bb6003ce511ca3d'])
        golive = str(org['24582ea974bfcb46c1985c3350d33acab5e54246'])[:10]
        signature = org['af6c7d5ca6bec13a3a2ac0ffe4f05ed98907c412']
        awarde = org['446585f9020fe3190ca0fa5ef53fc429ef4b4441']
        churn = org['eda2124e4e8bed55f7f2642cf3b5238d4bfccd58']
        fin_contrat = org['7381f1cd157f298aaf3b74f90f23cdb8a7cacda3']
        account_info = org.get('e058ea93145bdf66d23b89dfab0d8f74178bb23b', {})
        account_name = account_info.get('name') if account_info else None

        data.append({
            'societyName': society_name,
            'societyId': society_id,
            'company_status': actif,
            'company_golive': golive,
            'signature': signature,
            'fin_contrat': fin_contrat,
            'awarde': awarde,
            'account': account_name,
            'churn': churn
        })

    df = pd.DataFrame(data)
    df = df.astype(str)
    df.to_csv("csv/pipe_all.csv", index=False)

def variation():
    df = pd.read_csv("csv/conso.csv")
    import numpy as np

    def calculate_variation(row):
        if row['N-1'] == 0:
            if row['N'] == 0:
                return 0
            else:
                return np.inf  # Representing an infinite increase
        else:
            if row['N'] == 0:
                return -100.0
            else:
                return ((row['N'] - row['N-1']) / abs(row['N-1'])) * 100

    # Apply the function to each row
    df['Variation (%)'] = df.apply(calculate_variation, axis=1)
    df['Variation (%)'] = df['Variation (%)'].round(2)

    df.to_csv('csv/conso.csv', index=False)

def merge_all():
    import pandas as pd

    # Load the three CSV files
    conso_df = pd.read_csv('csv/conso.csv')
    base_df = pd.read_csv('csv/base.csv')
    pipe_all_df = pd.read_csv('csv/pipe_all.csv')

    # Merge the three DataFrames on 'societyId', giving priority to 'conso'
    merged_df = conso_df.merge(base_df, on='societyId', how='left').merge(pipe_all_df, on='societyId', how='left')

    # Drop duplicate columns if any (keeping the first occurrence)
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Save the merged result to a new CSV file
    merged_df.to_csv('csv/merged_result.csv', index=False)

    # Display the first few rows of the merged DataFrame
    merged_df.head()

def update_drive():
    print("########### GET UPDATEDRIVE START ###########")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import pandas as pd

    # Liste des tuples contenant les paires de (nom_fichier_csv, plage_sheet)
    list = [('merged_result.csv', 'conso_updated'),('tarif.csv', 'tarif_nego'),('warning.csv', 'warning'),('entities.csv', 'entities'),('entities_unactive.csv', 'entities_unactive')]

    # Fichier de compte de service et les scopes de l'API
    SERVICE_ACCOUNT_FILE = 'creds/n8n-api-311609-115ae3a49fd9.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def sheet_upt(filename, sheet_range):
        # ID de votre feuille Google Sheets
        SPREADSHEET_ID = '17VFyCP-CKmNl1X1BRVdc0jNjC4o9hyAopRrb5fITAK8'

        # Authentification et construction du service Google Sheets
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)

        # Lecture du DataFrame pivoté
        df = pd.read_csv(f'csv/{filename}')
        df = df.astype(str).replace('nan', '')

        if 'sub_price' in df.columns:
            df['sub_price'] = pd.to_numeric(df['sub_price'], errors='coerce')

        if '2022-2023' in df.columns:
            df['2022-2023'] = pd.to_numeric(df['2022-2023'], errors='coerce')

        if '2023-2024' in df.columns:
            df['2023-2024'] = pd.to_numeric(df['2023-2024'], errors='coerce')

        if '2024-2025' in df.columns:
            df['2024-2025'] = pd.to_numeric(df['2024-2025'], errors='coerce')

        # Remplacer les NaN par des chaînes vides pour éviter des erreurs dans l'API Google Sheets
        df = df.fillna("")

        df = df[df.columns.drop(df.filter(regex='_x$|_y$').columns)]

        # Convertir le DataFrame en une liste de listes (comme attendu par l'API Google Sheets)
        values = df.values.tolist()
        values.insert(0, df.columns.tolist())  # Ajouter les noms de colonnes en haut

        # Préparation des données pour l'API
        body = {
            'values': values
        }

        # Mise à jour de la feuille Google Sheets
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_range,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cellules mises à jour pour {filename} vers {sheet_range}.")

    # Boucle sur la liste des fichiers et plages
    for filename, sheet_range in list:
        sheet_upt(filename, sheet_range)

def envoi_email(status,error):
    SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']

    sender_email = 'ope@supertripper.com'
    sender_name = 'Supertripper Reports'
    recipient_email = "ope@supertripper.com"
    subject = f'CONSO BILLS : CRON {status}'

    # Construction du corps de l'e-mail
    body = (
        f'{error}'
    )
    creds_file = 'creds/cred_gmail.json'
    token_file = 'token.json'
    def authenticate_gmail():
        """Authentifie l'utilisateur via OAuth 2.0 et retourne les credentials"""
        creds = None
        # Le token est stocké localement après la première authentification
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        # Si le token n'existe pas ou est expiré, on initie un nouveau flux OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Enregistrer le token pour des sessions futures
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def create_message_with_attachment(sender, sender_name, to, subject, message_text):
        """Crée un e-mail avec une pièce jointe et un champ Cc"""
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = f'{sender_name} <{sender}>'
        message['subject'] = subject

        # Attacher le corps du texte
        message.attach(MIMEText(message_text, 'plain'))

        # Encoder le message en base64 pour l'envoi via l'API Gmail
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw_message}

    def send_email(service, user_id, message):
        """Envoie un e-mail via l'API Gmail"""
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print(f"Message Id: {message['id']}")
            return message
        except HttpError as error:
            print(f'An error occurred: {error}')
            return None

    # Authentifier l'utilisateur et créer un service Gmail
    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    # Créer le message avec pièce jointe et copie
    message = create_message_with_attachment(sender_email, sender_name, recipient_email, subject, body)

    # Envoyer l'e-mail
    send_email(service, 'me', message)
    print("Mail envoyé pour vérif ")

