from time import sleep
import random
import datetime
import hashlib

from melody_bridge import *


class QuizMaster:
    def __init__(self, lock, database, period_range=(10,30)):
        self.lock = lock
        self.database = database
        self.killed = False
        self.period_range = period_range
        self.last_time = datetime.datetime.now()

    def check_reevaluate_cost(self, video):
        self.lock.acquire()
        try:
            viddata = self.database["videos"][video]
            now = datetime.datetime.now()
            if "last_updated" not in viddata or now - datetime.datetime.fromisoformat(viddata["last_updated"]) > datetime.timedelta(minutes=1):
                self.database["videos"][video]["last_updated"] = now.isoformat()
                self.save_database()
                self.lock.release()
                fresh_data = get_metadata_from_file(video)
                self.lock.acquire()
                if fresh_data is None:
                    del self.database["videos"][video]
                else:
                    fresh_data["last_updated"] = now.isoformat()
                    self.database["videos"][video] = fresh_data
                self.save_database()
        except Exception as e:
            print(f"There was an error in check_reevaluate_cost: {e}")
        self.lock.release()

    def run_quiz(self):
        try:
            userlist = self.database["users"].keys()
            timeframe = datetime.datetime.now() - self.last_time
            self.last_time = datetime.datetime.now()
            for user in userlist:
                print(f"quizzing user {user}")
                userdata = self.database["users"][user]
                seeded_files = get_seeded_files(userdata["ip"])
                if seeded_files is None:
                    print(f"{user} is not seeding any files")
                    continue
                filtered_files = [seeded_file for seeded_file in seeded_files if seeded_file in self.database["videos"]]
                if filtered_files is None or len(filtered_files) < 1:
                    print(f"{user} is not seeding any files that are known to the authority")
                    continue
                quiz_choice = random.choice(filtered_files)
                validation = download_file(quiz_choice).content
                submission = get_data_from_client(userdata["ip"], quiz_choice).content
                val_hash = hashlib.sha256()
                val_hash.update(validation)
                sub_hash = hashlib.sha256()
                sub_hash.update(submission)
                if val_hash.digest() == sub_hash.digest():
                    score = len(filtered_files) * timeframe.total_seconds() * 0.01
                    print(f"User {user} passed a quiz and was awarded {score} points.")
                    self.lock.acquire()
                    self.database["users"][user]["points"] += score
                    self.save_database()
                    self.lock.release()
                else:
                    print(f"User {user} failed a quiz since {sub_hash.hexdigest()} is not equal to {val_hash.hexdigest()}.")
        except Exception as er:
            print(f"There was an error in run_quiz: {er}")


    def quiz_loop(self):
        print("starting quiz loop")
        while not self.killed:
            sleep(random.uniform(*self.period_range))
            print("running quiz protocol")
            self.run_quiz()

    def save_database(self):
        assert self.lock.locked()
        with open("database.json","w") as f:
            json.dump(self.database, f)

def start_quiz_loop(quizmaster):
    quizmaster.quiz_loop()


