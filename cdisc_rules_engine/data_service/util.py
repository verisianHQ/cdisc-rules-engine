import hashlib
import base64
import re


def generate_hash(input_string: str, length: int = 10) -> str:
    hash_bytes = hashlib.sha256(input_string.encode("utf-8")).digest()
    hash_b64 = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    alphanum_hash = re.sub(r"[^a-zA-Z0-9]", "", hash_b64).lower()

    clean_input = re.sub(r"[^a-zA-Z0-9]", "_", input_string).lower()
    if not clean_input or not clean_input[0].isalpha():
        clean_input = "v_" + clean_input
    clean_input = clean_input[:50]

    return f"{clean_input}_{alphanum_hash[:length]}"
