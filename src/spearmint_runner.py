import subprocess
import multiprocessing as mp
import time
import json
from datetime import datetime
import os
import shlex
import glob


def run(iterations, time_to_beat, duration, polling_frequency):

    # date_time = datetime.now()
    # subprocess.Popen(shlex.split("mkdir /Users/rahulbalakrishnan/Desktop/data/experiment_run-{}"
    #                              .format(date_time.strftime("%m-%d-%Y-%H-%M-%S"))))

    cumulative_results = []

    for _ in range(iterations):

        print("Starting iteration {}".format(_))

        # start_time = time.time()

        queue = mp.Queue()

        cmd = "python2.7 ./spearmint/spearmint/main.py --driver=local --method=GPEIOptChooser " + \
              "--method-args=noiseless=1 --data-file=test.csv /Users/rahulbalakrishnan/Desktop/" + \
              "throttlebot/src/spearmint/bayOptSearch/bayOpt.pb"

        p = subprocess.Popen(shlex.split(cmd), shell=False)



        process_poll = mp.Process(target=poll_for_best_result, args = (queue, time_to_beat, p, duration,
                                                                       polling_frequency))

        process_poll.start()

        process_poll.join()

        cumulative_results.append(queue.get())

        print("Done with iteration {}".format(_))


        subprocess.check_output(["bash ./spearmint/spearmint/save.sh"], shell=True)


    print("Saving aggregate results to disk")

    date_time = datetime.now()
    with open("/Users/rahulbalakrishnan/Desktop/data/threshold/data_{}"
                      .format(date_time.strftime("%m-%d-%Y-%H-%M-%S")), "w") as f:

            f.write(json.dumps({"results": cumulative_results, "polling_frequency": polling_frequency}))

def poll_for_best_result(queue, time_to_beat, process_to_terminate, duration, polling_frequency):

    starting_time = time.time()
    time_to_compare = starting_time
    while time.time() - starting_time < duration:


        try:

            current_time = time.time()
            if current_time - time_to_compare >= polling_frequency:
                time_to_compare = current_time
                cmd = "grep \'Best result\' /Users/rahulbalakrishnan/Desktop/throttlebot/src/spearmint/" \
                      "bayOptSearch/best_job_and_result.txt"

                output = str(subprocess.check_output([cmd], shell=True).decode("utf-8"))
                trial = len(glob.glob("/Users/rahulbalakrishnan/Desktop/throttlebot/src/spearmint/bayOptSearch/output/*"))
                queue.put([float(output[13:-1]), trial])

            else:
                time.sleep(max(polling_frequency, 2))

        except:
            time.sleep(max(polling_frequency, 2))




    process_to_terminate.kill()



run(iterations=20, time_to_beat=4000, duration=5*60, polling_frequency=30)