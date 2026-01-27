import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('MVS_DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL') or os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("WARNING: No database URL found. Set MVS_DATABASE_URL, DATABASE_PUBLIC_URL, or DATABASE_URL")
    print("Service will start but database operations will fail.")
    DATABASE_URL = None
