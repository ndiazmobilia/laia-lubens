import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

ENV_SUFFIX = '_GOOGLE_DRIVE_DIR'


def get_drive_sources() -> dict[str, str]:
    """
    Scans environment variables and returns a dictionary of sources
    that end with '_GOOGLE_DRIVE_DIR'.

    The key is the prefix of the environment variable (e.g., 'DATOS_PERSONALES'),
    and the value is the Google Drive folder URL.

    :return: A dictionary mapping source names to their URLs.
    """
    sources = {}
    for key, value in os.environ.items():
        if key.endswith(ENV_SUFFIX):
            # Extract the name part by removing the suffix
            name = key[:-len(ENV_SUFFIX)].lower()
            sources[name] = value
    return sources

# Load the sources on startup
DRIVE_SOURCES = get_drive_sources()
