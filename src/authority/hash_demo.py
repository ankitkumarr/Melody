from melody_bridge import *
import hashlib

def get_file_hash(node_ip, file_uuid):
    data = get_data_from_client(node_ip, file_uuid).content
    hashed = hashlib.sha256()
    hashed.update(data)
    print(hashed.hexdigest())
    return hashed.hexdigest()
