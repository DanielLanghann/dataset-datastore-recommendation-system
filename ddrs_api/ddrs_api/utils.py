"""
Script to generate a Fernet encryption key for Django datastore encryption
Run this once and add the output to your .env file
"""

from cryptography.fernet import Fernet

# Generate a new encryption key
key = Fernet.generate_key()
key_string = key.decode()

print("Generated Fernet encryption key:")
print("=" * 50)
print(key_string)
print("=" * 50)
print(f"\nAdd this line to your .env file:")
print(f"DATASTORE_ENCRYPTION_KEY={key_string}")
print("\nIMPORTANT:")
print("- Keep this key secure and secret")
print("- Never commit it to version control")
print("- If you lose this key, you cannot decrypt existing passwords")
print("- If you change this key, existing encrypted passwords will be unreadable")
