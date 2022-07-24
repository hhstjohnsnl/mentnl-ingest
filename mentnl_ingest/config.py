import os
from dotenv import load_dotenv


# Load .env file into environment variables
load_dotenv()


username = os.environ["USERNAME"]
personal_access_token = os.environ["PERSONAL_ACCESS_TOKEN"]

