import os
import random
import string
import math
import pymongo
import psycopg2

##############################################################################
# Configuration
##############################################################################
NUM_RECORDS = 500_000  # Increase/decrease for your scale tests
BATCH_SIZE = 5_000     # For Postgres inserts

##############################################################################
# Generate Random Data
##############################################################################
def random_name(length=7):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def generate_data(num_records=100_000):
    """
    Generate a list of dicts with fields: { name: <str>, age: <int> }
    Example: { 'name': 'abcxyz', 'age': 42 }
    """
    data_list = []
    for _ in range(num_records):
        name = random_name()
        age = random.randint(18, 90)
        data_list.append({"name": name, "age": age})
    return data_list

##############################################################################
# MongoDB: Load Sample Data
##############################################################################
mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
mongo_db_name = os.environ.get("DB_NAME", "testdb")

mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]

# Prepare large sample data
collection_name = "users"
print(f"Generating {NUM_RECORDS} records for Mongo '{collection_name}' collection...")
mongo_data = generate_data(NUM_RECORDS)

print("Inserting data into MongoDB...")
collection = mongo_db[collection_name]
# Clear existing data in Mongo
collection.delete_many({})
# Insert all in one go (Mongo can handle large arrays, but you could chunk it if desired)
collection.insert_many(mongo_data)
print(f"Inserted {NUM_RECORDS} records into MongoDB '{collection_name}' collection.")

##############################################################################
# Postgres: Load the Same Sample Data
##############################################################################
postgres_uri = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/testdb")

try:
    conn = psycopg2.connect(postgres_uri)
    conn.autocommit = True
    cursor = conn.cursor()

    print("Dropping and recreating 'users' table in Postgres...")
    cursor.execute("DROP TABLE IF EXISTS users;")
    cursor.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT
        );
    """)

    print(f"Inserting {NUM_RECORDS} records into Postgres (batch size = {BATCH_SIZE})...")
    # We'll batch the insert in chunks to avoid overly large statements
    total_batches = math.ceil(NUM_RECORDS / BATCH_SIZE)

    # Convert data for psycopg2's executemany
    # We only need 'name' and 'age' from each doc
    start_index = 0
    for batch_num in range(total_batches):
        batch_data = mongo_data[start_index:start_index + BATCH_SIZE]
        # Transform to tuples for executemany
        pg_data = [(doc["name"], doc["age"]) for doc in batch_data]
        cursor.executemany("INSERT INTO users (name, age) VALUES (%s, %s);", pg_data)
        start_index += BATCH_SIZE
        print(f"  -> Inserted batch {batch_num + 1}/{total_batches}")

    cursor.close()
    conn.close()
    print(f"Successfully loaded {NUM_RECORDS} records into Postgres 'users' table.")
except Exception as e:
    print(f"Error loading data into Postgres: {e}")
