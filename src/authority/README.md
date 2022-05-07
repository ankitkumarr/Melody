The Authority server is optionally run separately from the Melody instance.

Each Authority needs to be running Melody, but not every user running Melody needs to be hosting an Authority.


The Authority is a website that helps coordinate users who want to use Melody.  The Authority plays two major rolls:

1. To curate videos.  Videos an Authority presents should be a filtered subset of presumably only quality videos, compared to the entire contents of the DHT.
2. To enforce a level of replication by requiring users to seed and quizzing them.  An Authority should require clients to seed as a cost of using their services.


Authorities can come in many shapes and multiple Authorities should exist and compete.  In fact, it should be easy to make your own authority at any time.

Our sample Authority curates videos in the form of user-created and managed playlists.  It charges users "points" for acquiring the file UUID needed to download a file.  It gives users points for passing seed quizzes.  It grants double points and does not charge points for files that only have 1 seeder according to the DHT.


# How to run

1. Install the prerequisites:
`pip install flask`
`pip install requests`

2. Fill in the IP address of the Authority's Melody node.  Edit line 5 of melody_bridge.py (by default its set to `localhost:8001`).

3. Run the webserver:
`flask run`

# Features
- make an account or login (WARNING: no security is implemented, it just takes a username)
- enter the IP address of your client's Melody node to earn points for seeding
- view or make playlists
- add videos to playlists that you made
- charge points for downloading
- quiz system to earn points
