
import json
import csv
from datetime import datetime


def generateURL(docID, config, doctype=None):
    # Access values
    baseURL = config.get("baseURL")
    if doctype is None:
        doctype = config.get("doctype")
    apikey = config.get("apikey")

    #Generate URL
    url = baseURL + "/" + docID + "?type=" + doctype + "&Subscription-Key=" + apikey

    #Return URL
    return url

import csv

def json_list_to_csv(json_list, csv_filename):
    """
    Writes a list of JSON objects (dictionaries) to a CSV file.
    
    :param json_list: List of dictionaries containing JSON data
    :param csv_filename: Name of the CSV file to write to
    """
    if not json_list:
        print("Empty JSON list provided.")
        return
    
    # Extract keys from the first dictionary (assuming all have the same keys)
    keys = json_list[0].keys()
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(json_list)
    
    print(f"CSV file '{csv_filename}' has been created successfully.")


def get_latest_submit_datetime(csv_filename):
    """
    Reads a CSV file and returns the latest value in the 'submitDateTime' column.
    
    :param csv_filename: Location of the CSV file
    :return: Latest 'submitDateTime' value as a string
    """
    latest_datetime = None
    
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            
            for row in reader:
                submit_datetime_str = row.get('submitDateTime')
                if submit_datetime_str:
                    try:
                        submit_datetime = datetime.strptime(submit_datetime_str, "%Y-%m-%d %H:%M")
                        if latest_datetime is None or submit_datetime > latest_datetime:
                            latest_datetime = submit_datetime
                    except ValueError as e:
                        print(f"Error parsing date '{submit_datetime_str}': {e}")
    except FileNotFoundError:
        print(f"File '{csv_filename}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return latest_datetime.strftime("%Y-%m-%d %H:%M:%S") if latest_datetime else None

def get_list_of_Docs(csv_filename, edinetCode, docTypeCode):
    """
    Reads a CSV file and returns the list of document IDs.
    :param csv_filename: Location of the CSV file
    :param edinetCode: edinet code of the company
    :param docTypeCode: The document you want. 120 is for annual reports
    :return: List of document IDs
    """
    doc_list = []
    
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            
            for row in reader:
                doc_id = row.get('docID')
                if (doc_id 
                and edinetCode == row.get('edinetCode') 
                and docTypeCode == row.get('docTypeCode')):                    
                    doc_list.append(doc_id)
    except FileNotFoundError:
        print(f"File '{csv_filename}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return doc_list

def get_column_names(csv_file):
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='\t', quotechar='"')
        headers = next(reader)
        return headers