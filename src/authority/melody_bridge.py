# this file is supposed to hold all interaction with melody


# these methods are mostly used for an authority trying to index the DHT
def get_keywords_from_node(node_ip):
    return []

def get_ip_of_next_node(node_ip):
    return None

# these methods are mostly used for an authority trying to quiz a seeding client
def get_seeded_files(node_ip):
    return []

def get_data_from_client(node_ip, file_uuid):
    return None

# these methods are mostly used for an authority trying to serve content to a client
def get_metadata_from_file(file_uuid):
    return {"title" : "Video Title", "desc" : f"Video from uuid {file_uuid}.", "seeders" : ["127.0.0.1"]}
