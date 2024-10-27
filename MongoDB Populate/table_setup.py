import pandas as pd
import json
from pymongo import MongoClient


# Function to create a MongoDB client and connect to the database
def create_mongodb_connection(uri, db_name):
    """Create a connection to MongoDB and return the database."""
    client = MongoClient(uri)
    db = client[db_name]
    print(f"Connected to MongoDB database: {db_name}")
    return db


# Function to insert data from ecdict.csv into MongoDB
def insert_words_data(db, csv_file):
    """Insert data from ecdict.csv into 'words' collection."""
    ecdict_df = pd.read_csv(csv_file)
    words_data = ecdict_df.to_dict(orient='records')
    db.words.insert_many(words_data)
    print(f"Inserted data from {csv_file} into 'words' collection.")


# Function to insert data from lemma.en.txt into MongoDB
def insert_lemmas_data(db, lemma_file):
    """Insert data from lemma.en.txt into 'lemmas' collection."""
    lemmas_data = []
    with open(lemma_file, 'r') as file:
        for line in file:
            if "->" in line:
                lemma, forms = line.strip().split(" -> ")
                lemmas_data.append({"lemma": lemma, "forms": forms.split(", ")})
    db.lemmas.insert_many(lemmas_data)
    print("Inserted lemmas data into 'lemmas' collection.")


# Function to insert data from wordroot.txt into MongoDB
def insert_wordroots_data(db, wordroot_file):
    """Insert data from wordroot.txt into 'wordroots' collection."""
    with open(wordroot_file, 'r') as file:
        wordroot_data = json.load(file)
        formatted_data = [{"root": root, **details} for root, details in wordroot_data.items()]
    db.wordroots.insert_many(formatted_data)
    print("Inserted word roots data into 'wordroots' collection.")


# Function to insert data from resemble.txt into MongoDB
def insert_resemble_data(db, resemble_file):
    """Insert data from resemble.txt into 'resemble' collection."""
    resemble_data = []
    with open(resemble_file, 'r') as file:
        groups = file.read().split('%')
        for group in groups:
            if group.strip():
                lines = group.strip().splitlines()
                group_name = lines[0].strip()
                description = "\n".join(lines[1:]).strip()
                resemble_data.append({"group_name": group_name, "description": description})
    db.resemble.insert_many(resemble_data)
    print("Inserted resemble data into 'resemble' collection.")


# Main function to set up and populate the MongoDB database
def setup_mongodb(uri, db_name, csv_file, lemma_file, wordroot_file, resemble_file):
    """Populate MongoDB with data from all sources."""
    db = create_mongodb_connection(uri, db_name)
    insert_words_data(db, csv_file)
    insert_lemmas_data(db, lemma_file)
    insert_wordroots_data(db, wordroot_file)
    insert_resemble_data(db, resemble_file)
    print("MongoDB setup and population completed.")


# Example usage (adjust paths and URI as necessary)
if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    db_name = "en_cn_dict"
    csv_file = '/Users/lengocbinh/Documents/Koolsoft/ECDICT/ecdict.csv'
    lemma_file = '/Users/lengocbinh/Documents/Koolsoft/ECDICT/lemma.en.txt'
    wordroot_file = '/Users/lengocbinh/Documents/Koolsoft/ECDICT/wordroot.txt'
    resemble_file = '/Users/lengocbinh/Documents/Koolsoft/ECDICT/resemble.txt'

    setup_mongodb(uri, db_name, csv_file, lemma_file, wordroot_file, resemble_file)
