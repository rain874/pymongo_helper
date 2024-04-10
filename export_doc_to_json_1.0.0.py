# DocCOM: Das Script erstellt ein Ordner mit dem dbname und einen Ordner mit dem collectionname
# es exporiert jedes Dokument aus der gewünschten Collection in ein JSON file.

import pymongo
import json
import os 
from bson import ObjectId
import codecs

SCRIPT_FILE = os.path.realpath(__file__)
DIRECTORY = os.path.dirname(SCRIPT_FILE)

dbname = ''
collectionname = ''
mongoserver = 'mongodb://0.0.0.0:27017'

# Pfad zum neuen Ordner angeben
mkdir_dbname = "/" + dbname + "/"
mkdir_collectionname = mkdir_dbname + collectionname + "/"


# Prüfe ob der Ordner bereits vorhanden 
if os.path.exists(DIRECTORY + mkdir_dbname):
    print('Datenbank Ordner vorhanden')
else:
   # Datenbank Ordner erstellen
    os.mkdir(DIRECTORY + mkdir_dbname)

if os.path.exists(DIRECTORY + mkdir_collectionname):
    print('collection Ordner vorhanden')
else:
    # Collection Ordner erstellen
    os.mkdir(DIRECTORY + mkdir_collectionname)

print(mkdir_collectionname)


# Benutzerdefinierte Funktion zum Umwandeln von ObjectId in einen serialisierbaren String
def custom_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# Verbindung zur MongoDB aufbauen
client = pymongo.MongoClient(mongoserver)
db = client[dbname]
collection = db[collectionname]

# Alle Dokumente aus der Collection abrufen
documents = collection.find()

# Jedes Dokument in eine separate JSON-Datei exportieren
counter = 0
for doc in documents:
    file_name = f"{doc['_id']}.json"
    with codecs.open(DIRECTORY + mkdir_collectionname + file_name, 'w', encoding='utf-8') as file:
        json.dump(doc, file, default=custom_json_encoder, indent=4, ensure_ascii=False)
    counter += 1
    print(f"{counter}: {file_name}")


# Verbindung zur MongoDB trennen
client.close()
