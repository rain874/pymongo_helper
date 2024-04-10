import pymongo
import json
import os
from bson import ObjectId


SCRIPT_FILE = os.path.realpath(__file__)
DIRECTORY = os.path.dirname(SCRIPT_FILE)

dbname = ''
collectionname = ''

# Pfad zum neuen Ordner angeben
mkdir_dbname = "/" + dbname + "/"
mkdir_collectionname = DIRECTORY + mkdir_dbname + collectionname + "/"

# Verbindung zur MongoDB aufbauen
client = pymongo.MongoClient('mongodb://0.0.0.0:27017')
db = client[dbname]
collection = db[collectionname]

# Alle exportierten JSON-Dateien in einem Ordner durchgehen und importieren
import_folder = mkdir_collectionname
counter = 0
for file_name in os.listdir(import_folder):
    with open(os.path.join(import_folder, file_name), 'r', encoding='utf-8') as file:
        counter += 1
        try:
            # JSON-Datei laden
            doc = json.load(file)
            # ObjectId-Feld zu ObjectId umwandeln
            doc['_id'] = ObjectId(doc['_id'])
            # Versuchen, Dokument in die MongoDB Collection einzufügen
            collection.insert_one(doc)
            print(f"Dokument {counter}: {file_name} importieren")
        except pymongo.errors.DuplicateKeyError:
            print(f"Dokument {counter}: {file_name} bereits vorhanden, überspringe...")
        except Exception as e:
            print(f"Fehler beim Importieren von {counter}: {file_name}: {e}")
# Verbindung zur MongoDB trennen
client.close()


