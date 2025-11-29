import base64
import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

def laravel_decrypt(payload_base64: str, app_key_base64: str) -> str:
    print("SENHA RECEBIDA:", payload_base64)
    # Remove prefixo "base64:" da APP_KEY e decodifica
    app_key = base64.b64decode(app_key_base64.replace("base64:", ""))

    # Decodifica o payload (base64 → json)
    payload_json = base64.b64decode(payload_base64)
    payload = json.loads(payload_json)

    # Decodifica IV e valor criptografado, ambos são base64
    iv = base64.b64decode(payload["iv"])
    encrypted = base64.b64decode(payload["value"])

    # AES-256-CBC
    cipher = AES.new(app_key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(encrypted)

    # Remove padding PKCS7
    decrypted = unpad(decrypted, AES.block_size)

    return decrypted.decode("utf-8")



