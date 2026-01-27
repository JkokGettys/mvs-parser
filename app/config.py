import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('MVS_DATABASE_URL')

if not DATABASE_URL:
    raise ValueError('MVS_DATABASE_URL environment variable is required')
