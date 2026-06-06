import os
import json
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.exceptions import InvalidSignature

def generate_key_pair(node_id: str, dest_dir: str = "/auth"):
    os.makedirs(dest_dir, exist_ok=True)
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(os.path.join(dest_dir, f"{node_id}_private.pem"), "wb") as f: 
        f.write(private_pem)
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(os.path.join(dest_dir, f"{node_id}_public.pem"), "wb") as f:
        f.write(public_pem)

def load_private_key(node_id: str, dest_dir: str = "/auth"):
    path = os.path.join(dest_dir, f"{node_id}_private.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def load_public_key(node_id: str, dest_dir: str = "/auth"):
    path = os.path.join(dest_dir, f"{node_id}_public.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_public_key(f.read())

def sign_payload(private_key, payload: dict) -> bytes:
    serialized = json.dumps(payload, sort_keys=True).encode('utf-8')
    return private_key.sign(
        serialized,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

def verify_signature(public_key, payload: dict, signature: bytes) -> bool:
    serialized = json.dumps(payload, sort_keys=True).encode('utf-8')
    try:
        public_key.verify(
            signature,
            serialized,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),      
            hashes.SHA256()
        )
        return True
    except InvalidSignature:
        return False
