# crypto/crypto_utils.py

import logging
import os
import json
from pathlib import Path
from typing import Dict, Optional, Tuple, Union, Any
from base64 import b64encode, b64decode
from dataclasses import dataclass
from datetime import datetime, timedelta
import threading

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec, padding
from cryptography.hazmat.primitives.kdf import hkdf, pbkdf2
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.x509 import Certificate, SubjectAlternativeName, DNSName
from cryptography.x509.oid import NameOID
from cryptography.x509 import load_pem_x509_certificate

logger = logging.getLogger("CryptoUtils")

class CryptoException(Exception):
    """Base exception for cryptographic operations"""
    pass

class KeyGenerationError(CryptoException):
    """Failed to generate cryptographic keys"""
    pass

class EncryptionError(CryptoException):
    """Data encryption failure"""
    pass

class DecryptionError(CryptoException):
    """Data decryption failure"""
    pass

class SignatureError(CryptoException):
    """Digital signature processing error"""
    pass

@dataclass(frozen=True)
class KeyMetadata:
    key_id: str
    algorithm: str  # "RSA-4096", "ECDSA-P521", etc.
    created_at: datetime
    expires_at: Optional[datetime] = None
    tags: Dict[str, str] = None

class KeyManager:
    def __init__(self, keystore_dir: Path):
        self.keystore_dir = keystore_dir
        self.keystore_dir.mkdir(exist_ok=True)
        self._lock = threading.RLock()
        self._cache: Dict[str, bytes] = {}

    def generate_rsa_keypair(
        self, 
        key_size: int = 4096,
        public_exponent: int = 65537,
        key_id: Optional[str] = None,
        expiration_days: int = 365
    ) -> Tuple[str, KeyMetadata]:
        """Generate RSA key pair with metadata"""
        try:
            private_key = rsa.generate_private_key(
                public_exponent=public_exponent,
                key_size=key_size,
                backend=default_backend()
            )
            return self._store_keypair(private_key, "RSA", key_size, key_id, expiration_days)
        except Exception as e:
            logger.error(f"RSA keygen failed: {str(e)}")
            raise KeyGenerationError("RSA key generation failed") from e

    def generate_ecc_keypair(
        self,
        curve: ec.EllipticCurve = ec.SECP521R1(),
        key_id: Optional[str] = None,
        expiration_days: int = 365
    ) -> Tuple[str, KeyMetadata]:
        """Generate ECC key pair with metadata"""
        try:
            private_key = ec.generate_private_key(
                curve=curve,
                backend=default_backend()
            )
            curve_name = curve.name.split(":")[-1]
            return self._store_keypair(private_key, f"ECDSA-{curve_name}", None, key_id, expiration_days)
        except Exception as e:
            logger.error(f"ECC keygen failed: {str(e)}")
            raise KeyGenerationError("ECC key generation failed") from e

    def _store_keypair(
        self,
        private_key: Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey],
        algorithm: str,
        key_size: Optional[int],
        key_id: Optional[str],
        expiration_days: int
    ) -> Tuple[str, KeyMetadata]:
        with self._lock:
            key_id = key_id or os.urandom(16).hex()
            pem_data = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            metadata = KeyMetadata(
                key_id=key_id,
                algorithm=f"{algorithm}-{key_size}" if key_size else algorithm,
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(days=expiration_days)
            )
            
            key_path = self.keystore_dir / f"{key_id}.pem"
            meta_path = self.keystore_dir / f"{key_id}.meta"
            
            with open(key_path, "wb") as f:
                f.write(pem_data)
            
            with open(meta_path, "w") as f:
                json.dump({
                    "key_id": metadata.key_id,
                    "algorithm": metadata.algorithm,
                    "created_at": metadata.created_at.isoformat(),
                    "expires_at": metadata.expires_at.isoformat() if metadata.expires_at else None,
                    "tags": metadata.tags
                }, f)
            
            self._cache[key_id] = pem_data
            return key_id, metadata

    def get_public_key(self, key_id: str) -> bytes:
        """Retrieve public key in PEM format"""
        with self._lock:
            if key_id in self._cache:
                private_key = serialization.load_pem_private_key(
                    self._cache[key_id],
                    password=None,
                    backend=default_backend()
                )
                return private_key.public_key().public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )
            
            key_path = self.keystore_dir / f"{key_id}.pem"
            if not key_path.exists():
                raise KeyGenerationError(f"Key {key_id} not found")
            
            with open(key_path, "rb") as f:
                pem_data = f.read()
                self._cache[key_id] = pem_data
                private_key = serialization.load_pem_private_key(
                    pem_data,
                    password=None,
                    backend=default_backend()
                )
                return private_key.public_key().public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )

class CryptoProvider:
    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager
    
    def encrypt_asymmetric(
        self,
        plaintext: bytes,
        recipient_key_id: str,
        padding_type: padding.AsymmetricPadding = padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    ) -> bytes:
        """Encrypt data with recipient's public key"""
        try:
            public_key_pem = self.key_manager.get_public_key(recipient_key_id)
            public_key = serialization.load_pem_public_key(
                public_key_pem,
                backend=default_backend()
            )
            
            if isinstance(public_key, rsa.RSAPublicKey):
                return public_key.encrypt(plaintext, padding_type)
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                # ECIES encryption logic
                raise EncryptionError("EC encryption not implemented")
            else:
                raise EncryptionError("Unsupported public key type")
        except Exception as e:
            logger.error(f"Asymmetric encryption failed: {str(e)}")
            raise EncryptionError("Asymmetric encryption error") from e

    def decrypt_asymmetric(
        self,
        ciphertext: bytes,
        recipient_key_id: str,
        padding_type: padding.AsymmetricPadding = padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    ) -> bytes:
        """Decrypt data with private key"""
        try:
            private_key = self._load_private_key(recipient_key_id)
            
            if isinstance(private_key, rsa.RSAPrivateKey):
                return private_key.decrypt(ciphertext, padding_type)
            elif isinstance(private_key, ec.EllipticCurvePrivateKey):
                # ECIES decryption logic
                raise DecryptionError("EC decryption not implemented")
            else:
                raise DecryptionError("Unsupported private key type")
        except Exception as e:
            logger.error(f"Asymmetric decryption failed: {str(e)}")
            raise DecryptionError("Asymmetric decryption error") from e

    def encrypt_symmetric(
        self,
        plaintext: bytes,
        key: bytes,
        associated_data: Optional[bytes] = None
    ) -> Tuple[bytes, bytes]:
        """AEAD encryption using AES-GCM"""
        try:
            iv = os.urandom(12)
            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(iv),
                backend=default_backend()
            )
            encryptor = cipher.encryptor()
            
            if associated_data:
                encryptor.authenticate_additional_data(associated_data)
            
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            return (iv, ciphertext + encryptor.tag)
        except Exception as e:
            logger.error(f"Symmetric encryption failed: {str(e)}")
            raise EncryptionError("Symmetric encryption error") from e

    def decrypt_symmetric(
        self,
        ciphertext: bytes,
        key: bytes,
        iv: bytes,
        tag: bytes,
        associated_data: Optional[bytes] = None
    ) -> bytes:
        """AEAD decryption using AES-GCM"""
        try:
            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(iv, tag),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            
            if associated_data:
                decryptor.authenticate_additional_data(associated_data)
            
            return decryptor.update(ciphertext) + decryptor.finalize()
        except InvalidTag as e:
            logger.error("Decryption failed: Invalid authentication tag")
            raise DecryptionError("Authentication failed") from e
        except Exception as e:
            logger.error(f"Symmetric decryption failed: {str(e)}")
            raise DecryptionError("Symmetric decryption error") from e

    def _load_private_key(self, key_id: str) -> Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey]:
        try:
            public_key_pem = self.key_manager.get_public_key(key_id)
            return serialization.load_pem_private_key(
                self.key_manager._cache[key_id],
                password=None,
                backend=default_backend()
            )
        except Exception as e:
            logger.error(f"Private key load failed: {str(e)}")
            raise DecryptionError("Private key unavailable") from e

class SignatureManager:
    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager

    def sign_data(
        self,
        data: bytes,
        signer_key_id: str,
        hash_algorithm: hashes.HashAlgorithm = hashes.SHA256()
    ) -> bytes:
        """Generate digital signature"""
        try:
            private_key = self._load_private_key(signer_key_id)
            
            if isinstance(private_key, rsa.RSAPrivateKey):
                return private_key.sign(
                    data,
                    padding.PSS(
                        mgf=padding.MGF1(hash_algorithm),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hash_algorithm
                )
            elif isinstance(private_key, ec.EllipticCurvePrivateKey):
                return private_key.sign(
                    data,
                    ec.ECDSA(hash_algorithm)
                )
            else:
                raise SignatureError("Unsupported key type for signing")
        except Exception as e:
            logger.error(f"Signing failed: {str(e)}")
            raise SignatureError("Signature generation error") from e

    def verify_signature(
        self,
        data: bytes,
        signature: bytes,
        signer_key_id: str,
        hash_algorithm: hashes.HashAlgorithm = hashes.SHA256()
    ) -> bool:
        """Verify digital signature"""
        try:
            public_key_pem = self.key_manager.get_public_key(signer_key_id)
            public_key = serialization.load_pem_public_key(
                public_key_pem,
                backend=default_backend()
            )
            
            if isinstance(public_key, rsa.RSAPublicKey):
                public_key.verify(
                    signature,
                    data,
                    padding.PSS(
                        mgf=padding.MGF1(hash_algorithm),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hash_algorithm
                )
                return True
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                public_key.verify(
                    signature,
                    data,
                    ec.ECDSA(hash_algorithm)
                )
                return True
            else:
                raise SignatureError("Unsupported key type for verification")
        except InvalidSignature:
            return False
        except Exception as e:
            logger.error(f"Verification error: {str(e)}")
            raise SignatureError("Signature verification error") from e

# Example Usage
if __name__ == "__main__":
    km = KeyManager(Path("/tmp/keystore"))
    key_id, meta = km.generate_rsa_keypair()
    
    cp = CryptoProvider(km)
    ciphertext = cp.encrypt_asymmetric(b"Secret Message", key_id)
    plaintext = cp.decrypt_asymmetric(ciphertext, key_id)
    print(f"Decrypted: {plaintext.decode()}")
    
    sm = SignatureManager(km)
    data = b"Critical Transaction Data"
    signature = sm.sign_data(data, key_id)
    valid = sm.verify_signature(data, signature, key_id)
    print(f"Signature valid: {valid}")
