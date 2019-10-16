from datetime import datetime
import shlex
import subprocess
import json
import multiprocessing as mp
import time



def run(num_iterations, time_to_beat, duration, polling_frequency):


    for _ in range(num_iterations):

        # date_time = datetime.now()
        # file_name = ("/Users/rahulbalakrishnan/Desktop/data/tbotExperiment-{}".format(date_time.strftime("%m-%d-%Y-%H-%M-%S")))
        # subprocess.Popen(shlex.split("mkdir {}".format(file_name)))


        ps = subprocess.Popen(shlex.split("python2.7 run_throttlebot.py" +
                                                     " --config_file workload_config --time_to_beat {}"
                                                            .format(time_to_beat)), stdout=subprocess.PIPE)

        q = mp.Queue()
        polling_process = mp.Process(target=poll_for_results, args=(ps, duration, polling_frequency, q))

        polling_process.start()

        polling_process.join()


        # file_date_time = datetime.now()
        # with open("{}/{}".format(file_name, file_date_time.strftime("%m-%d-%Y-%H-%M-%S")), "w") as f:
        #     f.write(str_data)

    threshold_date_time = datetime.now()
    with open("/Users/rahulbalakrishnan/Desktop/data/tbot_threshold/{}"
                      .format(threshold_date_time.strftime("%m-%d-%Y-%H-%M-%S")), "w") as f:
        str_data = json.dumps([item for item in q])
        f.write(str_data)




def poll_for_results(process, duration, polling_frequency, queue):
    start = time.time()
    time_to_compare = start
    while time.time() - start < duration:

        try:
            current_time = time.time()
            if (current_time - time_to_compare >= polling_frequency):
                time_to_compare = current_time
                output = subprocess.check_output(shlex.split("grep \'Beat Time-to-beat with these stats\' | tail -n 1"),
                                                 stdin=process.stdout,
                                                 shell=False)
                output = str(output.decode("utf-8"))
                data = json.loads(output[output.index(": ") + 2:])

                queue.put(data)

            else:
                time.sleep(min(2, polling_frequency))
        except:
            time.sleep(min(2, polling_frequency))

    process.terminate()





run(num_iterations=2, time_to_beat=4000, duration=2*60)


