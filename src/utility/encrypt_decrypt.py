import base64
from dotenv import load_dotenv
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import os, sys
from loguru import logger
#load env 
load_dotenv()

try:
    key = os.getenv("APP_KEY")
    iv = os.getenv("APP_IV")
    salt = os.getenv("APP_SALT")

    if not (key and iv and salt):
        raise Exception(F"Error while fetching details for key/iv/salt")
except Exception as e:
    logger.error(f"Error occurred. Details: {e}")
    sys.exit(0)

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]

def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32


def encrypt(raw):
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))


def decrypt(enc):
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')



def main():
    decrypted = decrypt("Ew1qK/78m6udrJmIdmidnA==")
    print(decrypted)


if __name__ == "__main__":
    main()
