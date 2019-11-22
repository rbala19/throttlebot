from datetime import datetime
import shlex
import subprocess
import json
import multiprocessing as mp
import time
import logging


def run(num_iterations, time_to_beat, duration, polling_frequency):

    outer_result_list = []
    for _ in range(num_iterations):

        date_time = datetime.now()
        file_name = ("/Users/rahulbalakrishnan/Desktop/data/tbotExperiment-{}".format(date_time.strftime("%m-%d-%Y-%H-%M-%S")))
        subprocess.Popen(shlex.split("mkdir {}".format(file_name)))


        ps = subprocess.Popen(shlex.split("python2.7 run_throttlebot.py" +
                                                     " --config_file workload_config --time_to_beat {}"
                                                            .format(time_to_beat)))


        result_list = []
        start = time.time()
        time_to_compare = start
        first = True
        while time.time() - start < duration:
            try:
                current_time = time.time()
                # print("Current time is {} and time to compare is {}".format(current_time, time_to_compare))
                if (current_time - time_to_compare >= polling_frequency):

                    time_to_compare = current_time
                    output = subprocess.check_output("grep \'Beat Time-to-beat with these stats\' best_results | tail -n 1",
                                                 shell=True)
                    # output = ps.communicate()
                    # ps2 = subprocess.Popen(shlex.split("tail -n 1"), stdin = ps.stdout, shell=False)


                    # output = "test"
                    output = str(output.decode("utf-8"))[:-1]

                    print(output[output.index(": ") + 2:])
                    data = json.loads(output[output.index(": ") + 2:])
                    data.append(time.time() - start)

                    print("Data stored is {}".format(data))


                    if first:
                        start = current_time
                        first = False

                    result_list.append(data)

                else:
                    time.sleep(min(2, polling_frequency))
            except Exception as e:
                print("Error is {}".format(str(e)))
                time.sleep(min(2, polling_frequency))
                pass


        outer_result_list.append(result_list)

        ps.kill()



    threshold_date_time = datetime.now()
    with open("/Users/rahulbalakrishnan/Desktop/data/tbot_threshold/{}"
                      .format(threshold_date_time.strftime("%m-%d-%Y-%H-%M-%S")), "w") as f:
        str_data = json.dumps({"results": outer_result_list, "polling_frequency": polling_frequency, "duration": duration})
        f.write(str_data)



run(num_iterations=1, time_to_beat=10000, duration=45*60, polling_frequency=30)


