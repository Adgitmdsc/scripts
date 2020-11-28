import json
import random
import string
import sys

import firebase_admin
import pandas as pd
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

db = firestore.client()
users_ref = db.collection(u'certificates')

previous_id = []


def upload_data(output_file):
    df = pd.read_csv(f"{output_file}.csv")
    records = df.to_dict('records')

    for record in records:
        final_upload = {
            "one": record["one"],
            "two": record["two"],
            "three": record["three"],
            "four": record["four"],
            "five": record["five"]
        }
        record_uniq = record["UniqueId"]
        users_ref.document(record_uniq).set(final_upload)


def get_random_string(length):
    global previous_id
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    if result_str not in previous_id:
        return result_str
    else:
        get_random_string(12)


def generate_csv(read, output):
    df = pd.read_csv(f"{read}.csv")
    records = df.to_dict('records')
    for record in records:
        record["UniqueId"] = get_random_string(12)
        record["one"] = "This is to certify that"
        record["two"] = record["Name to be displayed on Certificate"]
        record[
            "three"] = "has attended Docker 101 on 22nd November 2020 which was organized by DSC ADGITM in association with DataQuest"
        record[
            "four"] = "https://firebasestorage.googleapis.com/v0/b/dsc-adgitm.appspot.com/o/dq_logo.png?alt=media&token=08315e12-7334-44d2-aae9-1d034abd71d6"
        record["five"] = "Certificate of Appreciation"

        record.pop("Timestamp")
    dataframe = pd.DataFrame(records)
    dataframe.to_csv(f"{output}.csv", index=False)


def load_keys(filename):
    global previous_id
    with open(filename) as json_file:
        data = json.load(json_file)
    previous_id += data["keys"]


if __name__ == '__main__':
    try:
        filename_output = sys.argv[1]
        filename_read = sys.argv[2]
    except:
        filename_read = "input"
        filename_output = "output"
    input_data = int(input("enter 1. to upload to firestore\n 2. for generating output file\n 3. for both generating and uploading"))
    if input_data == 1:
        print("uploading")
        upload_data(filename_output)
        print("done")
    if input_data == 2:
        load_keys("keys.json")
        generate_csv(filename_read, filename_output)
    if input_data == 3:
        load_keys("keys.json")
        generate_csv(filename_read, filename_output)
        print("uploading")
        upload_data(filename_output)
        print("done")
