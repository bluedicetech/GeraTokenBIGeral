import base64
import json
import hashlib
import hmac
from Crypto.Cipher import AES


def decrypt_laravel(encrypted_str: str, app_key_base64: str) -> str:
    # 1) App key do Laravel (sem "base64:")
    key = base64.b64decode(app_key_base64)

    # 2) Decodifica o payload base64
    payload = json.loads(base64.b64decode(encrypted_str))

    iv = base64.b64decode(payload["iv"])
    value = base64.b64decode(payload["value"])
    mac = payload["mac"]

    # 3) Verifica MAC (HMAC-SHA256)
    calc_mac = hmac.new(key, iv + base64.b64decode(payload["value"]), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_mac, mac):
        raise ValueError("MAC inválido — dados foram alterados ou chave incorreta.")

    # 4) Descriptografa AES-256-CBC
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(value)

    # 5) Remove PKCS#7 Padding
    pad_len = decrypted[-1]
    decrypted = decrypted[:-pad_len]

    return decrypted.decode("utf-8")