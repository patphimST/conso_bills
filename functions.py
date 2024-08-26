from datetime import datetime, timezone
from pymongo import MongoClient
import config
import re
import requests
import pandas as pd
from config import api_pipedrive

client = MongoClient(
    f'mongodb+srv://{config.mongo_pat}',
    tls=True,
    tlsAllowInvalidCertificates=True
)

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

    # Si la colonne _id contient des dictionnaires
    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: x if isinstance(x, dict) else {})
        df = df.join(pd.json_normalize(df['_id'])).drop(columns=['_id'])

    # Renommer les colonnes
    df.rename(columns={'societyId': 'societyId', 'yearMonth': 'yearMonth'}, inplace=True)

    # Grouper par societyName et trier par yearMonth

    df['yearMonth'] = pd.to_datetime(df['yearMonth'], format='%Y-%m')

    df = df.sort_values(by=['societyName', 'yearMonth'])

    # Convertir yearMonth en datetime
    df['yearMonth'] = pd.to_datetime(df['yearMonth'])

    # Générer toutes les dates entre 2022-10-01 et 2024-09-01
    all_dates = pd.date_range(start="2022-10-01", end="2024-09-01", freq='MS')

    # Créer un DataFrame avec toutes les combinaisons de societyName, societyId, et yearMonth
    society_info = df[['societyName', 'societyId']].drop_duplicates()
    expanded_df = pd.DataFrame([(name, sid, date) for (name, sid) in society_info.values for date in all_dates],
                               columns=['societyName', 'societyId', 'yearMonth'])

    # Fusionner avec les données existantes
    final_df = pd.merge(expanded_df, df, on=['societyName', 'societyId', 'yearMonth'], how='left')

    # Remplir les colonnes N-1 et N
    start_n1 = pd.to_datetime("2022-10-01")
    end_n1 = pd.to_datetime("2023-09-30")
    start_n = pd.to_datetime("2023-10-01")
    end_n = pd.to_datetime("2024-09-30")

    final_df['N-1'] = final_df.apply(lambda row: row['totalAmount'] if start_n1 <= row['yearMonth'] <= end_n1 else None,
                                     axis=1)
    final_df['N'] = final_df.apply(lambda row: row['totalAmount'] if start_n <= row['yearMonth'] <= end_n else None,
                                   axis=1)

    # Afficher le DataFrame final
    final_df = final_df.drop(columns=['totalAmount'])
    final_df[['N','N-1']] = final_df[['N','N-1']].astype(float)
    import locale

    # Paramétrer la locale pour le français
    locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')  # Pour les systèmes Linux/MacOS
    # locale.setlocale(locale.LC_TIME, 'French_France.1252')  # Pour les systèmes Windows

    # Ajouter une colonne 'Month' qui extrait le mois en toutes lettres
    final_df['Month'] = final_df['yearMonth'].dt.strftime('%B')
    final_df.drop(columns='yearMonth', inplace = True)

    # Perform group by societyId and Month, and aggregate the data (e.g., sum or mean depending on context)
    month_order = [
        "octobre", "novembre", "décembre", "janvier", "février", "mars",
        "avril", "mai", "juin", "juillet", "août", "septembre"
    ]
    final_df['Month'] = pd.Categorical(final_df['Month'], categories=month_order, ordered=True)

    # Now, sort the grouped dataframe by societyId and Month
    grouped_df = final_df.groupby(['societyName', 'societyId', 'Month'], observed=True).sum().reset_index()
    month_mapping = {
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

    # Apply the mapping to the 'Month' column
    grouped_df['Month'] = grouped_df['Month'].map(month_mapping)

    grouped_df.to_csv('csv/conso.csv', index=False)

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

    # Aggregation pipeline
    result = client['legacy-api-management']['societies'].aggregate(
        [
            {
                '$unwind': '$billings'
            }, {
            '$project': {
                'name': 1,
                '_id': 1,
                "raison" : "$billings.raison",
                "address" : "$billings.address.label",
                "service" : "$billings.service",
                "mandatId" : "$billings.mandatId",
                "mandat_status" : "$billings.status",
                "amex" : "$settings.amex.cardHolderName",
            }
        }
        ]
    )

    # Convert result to DataFrame
    df = pd.DataFrame(list(result))

    # Replace '+Simple' with 'Plus Simple' in 'name' column
    df['name'] = df['name'].str.replace('+Simple', 'Plus Simple')
    df.rename(columns={'_id': 'societyId', 'name': 'societyName'}, inplace=True)

    # Création de la colonne "payment" avec les conditions
    def remplir_payment(row):
        if pd.notna(row['mandatId']) and pd.isna(row['amex']):
            return "mandat"
        elif pd.notna(row['amex']):
            return "carte logée"
        else:
            return "virement"

    # Appliquer la fonction à chaque ligne du DataFrame pour créer une nouvelle colonne "payment"
    df['payment'] = df.apply(remplir_payment, axis=1)


    # Save DataFrame to CSV
    df.to_csv('csv/entities.csv', index=False)

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
        account_info = org.get('e058ea93145bdf66d23b89dfab0d8f74178bb23b', {})
        account_name = account_info.get('name') if account_info else None

        data.append({
            'societyName': society_name,
            'societyId': society_id,
            'company_status': actif,
            'company_golive': golive,
            'signature': signature,
            'awarde': awarde,
            'account': account_name
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


def update_drive():
    print("########### GET UPDATEDRIVE START ###########")

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import pandas as pd

    # Liste des tuples contenant les paires de (nom_fichier_csv, plage_sheet)
    list = [('conso.csv', 'conso_updated'), ('pipe_all.csv', 'pipe'), ('base.csv', 'base'),('tarif.csv', 'tarif_nego'),('entities.csv', 'entities')]

    # Fichier de compte de service et les scopes de l'API
    SERVICE_ACCOUNT_FILE = 'creds/n8n-api-311609-115ae3a49fd9.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def sheet_upt(filename, sheet_range):
        # ID de votre feuille Google Sheets
        SPREADSHEET_ID = '1Ybw4aicEo2IgzxSYKWbDpZ3afW8GrjdzIeBhcrWNnGY'

        # Authentification et construction du service Google Sheets
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)

        # Lecture du DataFrame pivoté
        df = pd.read_csv(f'csv/{filename}')
        df = df.astype(str).replace('nan', '')

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

