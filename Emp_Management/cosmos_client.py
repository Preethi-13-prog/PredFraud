import os
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

load_dotenv()

COSMOS_CONNECTION_STRING = os.getenv("COSMOS_CONNECTION_STRING")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")


def get_container(container_name="EmployeeData"):

    if not COSMOS_CONNECTION_STRING:
        raise ValueError("COSMOS_CONNECTION_STRING missing")

    client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)

    db = client.get_database_client(COSMOS_DATABASE)

    return db.get_container_client(container_name)