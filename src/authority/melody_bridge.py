import requests, json, random
# this file is supposed to hold all interaction with melody
def make_url(ip, target):
    return f"http://{ip}/{target}"
localhost = "localhost:8001"

# these methods are mostly used for an authority trying to index the DHT
def get_keywords_from_node(node_ip):
    # ip/getlocalkeywords
    return []

def get_ip_of_next_node(node_ip):
    # ip/getnextnodes
    return None

# these methods are mostly used for an authority trying to quiz a seeding client
def get_seeded_files(node_ip):
    # getfileseeding
    try:
        data = json.loads(requests.get(make_url(node_ip, "getfilesseeding")).text)
        if len(data) < 1:
            return None
        return data
    except:
        return None

def get_data_from_client(node_ip, file_uuid):
    # getfile?fileid=<>
    try:
        data = requests.get(make_url(node_ip, f"getfile?fileid={file_uuid}"))
        return data
    except:
        return None

def download_file(file_uuid):
    try:
        data = json.loads(requests.get(make_url(localhost, f"/findfile?fileId={file_uuid}")).text)
        if data["Metadata"]["Id"] != file_uuid:
            return None
        seeder = random.choice(data["Seeders"])
        return get_data_from_client(seeder, file_uuid)
    except:
        return None

# these methods are mostly used for an authority trying to serve content to a client
def get_metadata_from_file(file_uuid):
    # findfile?fileId=<>
    try:
        data = json.loads(requests.get(make_url(localhost, f"/findfile?fileId={file_uuid}")).text)
        if data["Metadata"]["Id"] != file_uuid:
            return None
        outdata = {"title" : data["Metadata"]["Title"], "seeders" : data["Seeders"], "desc" : f"Description of video {data['Metadata']['Title']} would go here if the DHT supported it."}
        return outdata
    except:
        return None
    return {"title" : "Video Title", "desc" : f"Video from uuid {file_uuid}.", "seeders" : ["127.0.0.1"]}
