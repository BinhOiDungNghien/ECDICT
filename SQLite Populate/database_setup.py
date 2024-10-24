import sqlite3
import pandas as pd
import json


# Function to create a connection to the SQLite database
def create_connection(db_file):
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connection to {db_file} successful.")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn


# Function to create tables if they don't exist
def create_tables(conn):
    """Create necessary tables in the database."""
    cursor = conn.cursor()

    # Create words table
    cursor.execute('''CREATE TABLE IF NOT EXISTS words (
        word TEXT PRIMARY KEY,
        phonetic TEXT,
        definition TEXT,
        translation TEXT,
        pos TEXT,
        collins INTEGER,
        oxford INTEGER,
        tag TEXT,
        bnc INTEGER,
        frq INTEGER,
        exchange TEXT,
        detail TEXT,
        audio TEXT
    )''')

    # Create lemmas table
    cursor.execute('''CREATE TABLE IF NOT EXISTS lemmas (
        lemma TEXT PRIMARY KEY,
        forms TEXT
    )''')

    # Create wordroots table
    cursor.execute('''CREATE TABLE IF NOT EXISTS wordroots (
        root TEXT PRIMARY KEY,
        meaning TEXT,
        class TEXT,
        examples TEXT,
        origin TEXT
    )''')

    # Create resemble table
    cursor.execute('''CREATE TABLE IF NOT EXISTS resemble (
        group_name TEXT PRIMARY KEY,
        description TEXT
    )''')

    conn.commit()


# Function to insert data into words table
def insert_words_data(conn, csv_file):
    """Insert data from ecdict.csv into words table."""
    try:
        ecdict_df = pd.read_csv(csv_file)
        ecdict_df.to_sql('words', conn, if_exists='append', index=False)
        print(f"Data from {csv_file} successfully inserted into 'words' table.")
    except Exception as e:
        print(f"Error inserting words data: {e}")


# Function to insert data into lemmas table
def insert_lemmas_data(conn, lemma_file):
    """Insert data from lemma.en.txt into lemmas table."""
    cursor = conn.cursor()
    try:
        with open(lemma_file, 'r') as file:
            for line in file:
                if "->" in line:
                    lemma, forms = line.strip().split(" -> ")
                    cursor.execute("INSERT OR IGNORE INTO lemmas (lemma, forms) VALUES (?, ?)", (lemma, forms))
        conn.commit()
        print(f"Lemmas data successfully inserted into 'lemmas' table.")
    except Exception as e:
        print(f"Error inserting lemmas data: {e}")


# Function to insert data into wordroots table
def insert_wordroots_data(conn, wordroot_file):
    """Insert data from wordroot.txt into wordroots table."""
    cursor = conn.cursor()
    try:
        with open(wordroot_file, 'r') as file:
            wordroot_data = json.load(file)
            for root, details in wordroot_data.items():
                cursor.execute('''INSERT OR IGNORE INTO wordroots (root, meaning, class, examples, origin)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (root, details['meaning'], details['class'], ', '.join(details['example']),
                                details['origin']))
        conn.commit()
        print(f"Wordroots data successfully inserted into 'wordroots' table.")
    except Exception as e:
        print(f"Error inserting wordroots data: {e}")


# Function to insert data into resemble table
def insert_resemble_data(conn, resemble_file):
    """Insert data from resemble.txt into resemble table."""
    cursor = conn.cursor()
    try:
        with open(resemble_file, 'r') as file:
            groups = file.read().split('%')  # Split based on the delimiter '%'
            for group in groups:
                if group.strip():
                    lines = group.strip().splitlines()
                    group_name = lines[0].strip()  # The first line as the group name
                    description = "\n".join(lines[1:]).strip()  # Remaining lines as description
                    cursor.execute("INSERT OR IGNORE INTO resemble (group_name, description) VALUES (?, ?)", (group_name, description))
        conn.commit()
        print(f"Resemble data successfully inserted into 'resemble' table.")
    except Exception as e:
        print(f"Error inserting resemble data: {e}")



# Function to maintain the database, ensuring everything is set up correctly
def setup_database(db_file, csv_file, lemma_file, wordroot_file, resemble_file):
    """Main function to set up and populate the database."""
    conn = create_connection(db_file)
    if conn is not None:
        create_tables(conn)
        insert_words_data(conn, csv_file)
        insert_lemmas_data(conn, lemma_file)
        insert_wordroots_data(conn, wordroot_file)
        insert_resemble_data(conn, resemble_file)
        conn.close()
        print("Database setup and population completed.")
    else:
        print("Failed to create the database connection.")


# Example usage (adjust file paths as necessary)
if __name__ == "__main__":
    db_file = 'en_cn_dict.db'
    csv_file = 'C:/Users/PLC/Documents/Koolsoft/ECDICT/ecdict.csv'
    lemma_file = 'C:/Users/PLC/Documents/Koolsoft/ECDICT/lemma.en.txt'
    wordroot_file = 'C:/Users/PLC/Documents/Koolsoft/ECDICT/wordroot.txt'
    resemble_file = 'C:/Users/PLC/Documents/Koolsoft/ECDICT/resemble.txt'

    setup_database(db_file, csv_file, lemma_file, wordroot_file, resemble_file)
