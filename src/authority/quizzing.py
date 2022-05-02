from time import sleep
import random
import datetime

from melody_bridge import *


class QuizMaster:
    def __init__(self, lock, database, period_range=(1,4)):
        self.lock = lock
        self.database = database
        self.killed = False
        self.period_range = period_range
        self.last_time = datetime.datetime.now()

    def check_reevaluate_cost(self, video):
        self.lock.acquire()
        viddata = self.database["videos"][video]
        now = datetime.datetime.now()
        if "last_updated" not in viddata or now - viddata["last_updated"] > timedelta(minutes=1):
            self.database["videos"][video]["last_updated"] = now
            self.save_database()
            self.lock.release()
            fresh_data = get_metadata_from_file(video)
            self.lock.acquire()
            if fresh_data is None:
                del self.database["videos"][video]
            else:
                fresh_data["last_updated"] = now
                self.database["videos"][video] = fresh_data
            self.save_database()
        self.lock.release()

    def run_quiz(self):
        userlist = self.database["users"].keys()
        timeframe = datetime.datetime.now() - self.last_time
        self.last_time = datetime.datetime.now()
        for user in userlist:
            userdata = self.database["users"][user]
            seeded_files = get_seeded_files(userdata["ip"])
            filtered_files = [seeded_file for seeded_file in seeded_files if seeded_file in self.database["videos"]]
            if filtered_files is None or len(filtered_files) < 1:
                continue
            quiz_choice = random.choice(filtered_files)
            validation = download_file(quiz_choice)
            submission = get_data_from_client(userdata["ip"], quiz_choice)
            if submission == validation:
                score = len(filtered_files) * timeframe.total_seconds() * 0.01
                self.lock.acquire()
                self.database["users"][user]["points"] += score
                self.save_database()
                self.lock.release()


    def quiz_loop(self):
        while not self.killed:
            sleep(random.uniform(*self.period_range))
            self.run_quiz()

    def save_database(self):
        assert selflock.locked()
        with open("database.json","w") as f:
            json.dump(self.database, f)

def start_quiz_loop(quizmaster):
    quizmaster.quiz_loop()


