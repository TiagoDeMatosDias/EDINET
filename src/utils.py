
import json
import csv
from datetime import datetime


def generateURL(docID, config, doctype=None):
    """Build and return the full EDINET API URL for a given document.

    Args:
        docID (str): The EDINET document identifier.
        config (Config): The application config object used to retrieve base
            URL, document type, and API key.
        doctype (str, optional): Override for the document type. When omitted,
            the value from config is used.

    Returns:
        str: The fully constructed API URL including query parameters.
    """
    baseURL = config.get("baseURL")
    if doctype is None:
        doctype = config.get("doctype")
    apikey = config.get("API_KEY")

    url = baseURL + "/" + docID + "?type=" + doctype + "&Subscription-Key=" + apikey
    return url


def json_list_to_csv(json_list, csv_filename):
    """
    Writes a list of JSON objects (dictionaries) to a CSV file.

    Args:
        json_list (list): List of dictionaries containing JSON data.
        csv_filename (str): Name of the CSV file to write to.

    Returns:
        None
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

    Args:
        csv_filename (str): Location of the CSV file.

    Returns:
        str: Latest 'submitDateTime' value formatted as 'YYYY-MM-DD HH:MM:SS',
            or None if no valid datetime was found.
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
    Reads a CSV file and returns a list of document IDs matching the given filters.

    Args:
        csv_filename (str): Location of the CSV file.
        edinetCode (str): EDINET code of the company to filter by.
        docTypeCode (str): Document type code to filter by (e.g. '120' for annual reports).

    Returns:
        list: List of matching document ID strings.
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
    """Return the header row of a tab-delimited CSV file.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        list: List of column name strings from the first row of the file.
    """
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='\t', quotechar='"')
        headers = next(reader)
        return headers