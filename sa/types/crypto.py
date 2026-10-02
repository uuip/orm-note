"""
AES-GCM 加密与解密工具。

ENCRYPTION_KEY 应为经过 URL 安全 Base64 编码的 AES 密钥，原始长度为 16、24 或 32 字节。
可用 base64.urlsafe_b64encode(os.urandom(32)).decode() 生成一次，
此后保留并复用同一密钥，以便解密已保存的数据。
"""

import base64
import os
from typing import Union

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from config import settings

MAGIC_BYTES = b"AESV1GCM"  # 8 字节的格式标识

# GCM 参数
NONCE_SIZE = 12  # GCM 推荐的随机数长度，单位为字节
TAG_SIZE = 16  # 认证标签长度，单位为字节


def get_key() -> bytes:
    if settings.encryption_key is None:
        raise ValueError("ENCRYPTION_KEY is required for encryption and decryption")
    return base64.urlsafe_b64decode(settings.encryption_key.encode())


def encrypt(data: Union[str, bytes]) -> str:
    """
    使用 AES-GCM 加密数据。
    返回格式：base64(MAGIC_BYTES + nonce + ciphertext + tag)
    """
    if isinstance(data, str):
        data = data.encode()

    key = get_key()
    nonce = os.urandom(NONCE_SIZE)

    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce))
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(data) + encryptor.finalize()

    encrypted_data = MAGIC_BYTES + nonce + ciphertext + encryptor.tag
    return base64.urlsafe_b64encode(encrypted_data).decode()


def decrypt(encrypted_data: Union[str, bytes]) -> str:
    if isinstance(encrypted_data, str):
        try:
            binary_data = base64.urlsafe_b64decode(encrypted_data.encode())
        except Exception:
            raise ValueError("Invalid encrypted data format")
    else:
        binary_data = encrypted_data

    min_length = len(MAGIC_BYTES) + NONCE_SIZE + TAG_SIZE
    if len(binary_data) < min_length or not binary_data.startswith(MAGIC_BYTES):
        raise ValueError("Encrypted data format is invalid or corrupted")

    offset = len(MAGIC_BYTES)
    nonce = binary_data[offset : offset + NONCE_SIZE]
    tag = binary_data[-TAG_SIZE:]
    ciphertext = binary_data[offset + NONCE_SIZE : -TAG_SIZE]

    key = get_key()
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag))
    decryptor = cipher.decryptor()

    try:
        plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        return plaintext.decode()
    except Exception as e:
        raise ValueError(f"Decryption failed: {str(e)}")


def is_encrypted(data: str) -> bool:
    try:
        decoded = base64.urlsafe_b64decode(data.encode())
        return len(decoded) >= len(MAGIC_BYTES) and decoded.startswith(MAGIC_BYTES)
    except Exception:
        return False


if __name__ == "__main__":
    encrypted = encrypt("thisisapassword")
    print(f"Encrypted: {encrypted}")
    decrypted = decrypt(encrypted)
    print(f"Decrypted: {decrypted}")
    print(f"Verification: {'thisisapassword' == decrypted}")
    print(is_encrypted("9075bdb9b38e841687c1f574819f07ccd788f6a14cc2980cfd178c3cafd94a99"))
