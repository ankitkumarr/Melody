from flask import Flask, render_template, request, jsonify, make_response, abort, redirect, send_file
import json, requests, io
from threading import Lock, Thread
from melody_bridge import *
from quizzing import *

app = Flask(__name__)

@app.route('/')
@app.route('/index')
@app.route('/about')
def index():
    playlists = None
    userdata = prep_user_data(request)
    if "playlists" in database and len(database["playlists"]) > 0:
        playlists = database["playlists"]
    data = dict()
    if playlists is not None:
        data["playlists"] = playlists
    return render_template("homepage.html", userdata=userdata, data=data)



@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        resp = make_response(redirect("/"))
        data = request.form
        username = data['username']
        if not username in database["users"]:
            datalock.acquire()
            if not username in database["users"]:
                database["users"][username] = {"points" : 0, "ip" : request.remote_addr}
            save_database()
            datalock.release()
        resp.set_cookie("username",username)
        return resp
    else:
        resp = make_response(render_template("login.html"))
        if "logout" in request.args:
            resp.delete_cookie("username")
        return resp

@app.route('/playlists', defaults={'playlist' : None}, methods=['GET','POST'])
@app.route('/playlists/<playlist>', methods=['GET', 'POST'])
def playlists(playlist):
    userdata = prep_user_data(request)
    error = None
    if "error" in request.args:
        error = request.args["error"]
    if playlist is None:
        if request.method == "POST":
            if "username" not in userdata:
                return redirect("/login")
            title = None
            if "playlist_title" in request.form:
                title = request.form["playlist_title"]

            if title is None or title == "":
                error = "No title given."
            else:
                if title in database["playlists"]:
                    error = "Playlist already exists."
                else:
                    datalock.acquire()
                    database["playlists"][title] = {"creator" : userdata["username"], "videos" : []}
                    save_database()
                    datalock.release()
                    return redirect(f"/playlists/{title}")
            return render_template("make_playlist.html", userdata=userdata, error=error)

        elif "action" in request.args and request.args["action"] == "makenew":
            if "username" not in userdata:
                return redirect("/login")
            return render_template("make_playlist.html", userdata=userdata, error=error)
        else:
            return redirect("/")
    else:
        if playlist not in database["playlists"]:
            return redirect("/playlists?action=makenew&error=Playlist not found, but you can create it here.")
        playlistdata = database["playlists"][playlist]
        if request.method == "POST":
            datalock.acquire()
            playlistdata = database["playlists"][playlist]
            data = request.form
            if playlistdata["creator"] != userdata["username"]:
                error = "Only the creator of a playlist is allowed to edit it."
            elif "file_uuid" not in data or data["file_uuid"] == "" or data["file_uuid"] is None:
                error = "Please enter the file uuid."
            else:
                viddata = get_metadata_from_file(data["file_uuid"])
                if viddata is None or "title" not in viddata:
                    error = f"No file for uuid {data['file_uuid']} exists in the Melody DHT."
                else:
                    vidindex = -1
                    try:
                        vidindex = database["file_uuid_list"].index(data["file_uuid"])
                    except:
                        database["file_uuid_list"].append(data["file_uuid"])
                        vidindex = database["file_uuid_list"].index(data["file_uuid"])
                    database["videos"][data["file_uuid"]] = viddata
                    if vidindex in playlistdata["videos"]:
                        error = "That video is already in this playlist!"
                    else:
                        database["playlists"][playlist]["videos"].append(vidindex)
                    save_database()
                    playlistdata = database["playlists"][playlist]
            datalock.release()
        playlistdata = playlistdata.copy()
        playlistdata["title"] = playlist
        if len(playlistdata["videos"]) == 0:

            del playlistdata["videos"]
        return render_template("playlist.html", userdata=userdata, playlistdata=playlistdata, error=error, videodata=database["videos"], video_uuids=database["file_uuid_list"])


@app.route('/downloads/<playlist>/<index>')
def download(playlist, index):
    #TODO this code has an edge case where a user could end up paying a different cost from what they agreed to
    # I have ideas for fixing it (e.g. a system where a user locks in a cost the first time they visit the page)
    # but I'm considering it unimportant for an initial demo
    userdata = prep_user_data(request)
    error = None
    if "username" not in userdata:
        return redirect("/login")
    playlistdata = database["playlists"][playlist]
    videodata = database["videos"][database["file_uuid_list"][playlistdata["videos"][int(index)]]]
    quizmaster.check_reevaluate_cost(database["file_uuid_list"][playlistdata["videos"][int(index)]])
    cost = 0 if len(videodata["seeders"]) < 2 else 1
    if "cost_accepted" in request.args and request.args["cost_accepted"] == "true":
        datalock.acquire()
        if userdata["points"] < cost and False: # allowing points to go negative until I have implemented ways for users to seed before going negative
            error = "Not enough points!  Seed files to gather more points!"
            datalock.release()
        else:
            userdata["points"] -= cost
            data = io.BytesIO()
            data.write(database["file_uuid_list"][playlistdata["videos"][int(index)]].encode())
            data.seek(0)
            save_database()
            datalock.release()
            return send_file(data, as_attachment=True, attachment_filename=f"{videodata['title']}.melody", mimetype="text/csv")
    return render_template("download.html", playlisttitle=playlist, index=index, videodata=videodata, userdata=userdata, error=error, cost=cost)



def prep_user_data(request):
    username = request.cookies.get("username")
    if username is None or username not in database["users"]:
        return dict()
    else:
        userdata = database["users"][username]
        userdata["username"] = username
        return userdata


# function to save the database to disk
def save_database():
    assert datalock.locked()
    with open("database.json","w") as f:
        json.dump(database, f)




# prepare and load the database and stuf
database = dict()
datalock = Lock()
try:
    with open("database.json", "r") as f:
        database = json.load(f)
except:
    pass
if "users" not in database:
    database["users"] = dict()
if "playlists" not in database:
    database["playlists"] = dict()
if "videos" not in database:
    database["videos"] = dict()
if "file_uuid_list" not in database:
    database["file_uuid_list"] = list()


quizmaster = QuizMaster(datalock, database)
quiz_thread = Thread(target=start_quiz_loop, args=(quizmaster,))
quiz_thread.start()
