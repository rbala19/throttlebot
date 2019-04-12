import subprocess
from remote_execution import *
from kubernetes import client, config
import yaml
import os
from poll_cluster_state import *
from run_experiment import *
from collections import defaultdict
import logging
from workload_manager import *
import json
import multiprocessing as mp

def create_autoscaler(deployment_name, cpu_percent, min_size, max_size):

    output = subprocess.check_output("kubectl autoscale deployment {} --cpu-percent={} --min={} --max={}".format(deployment_name,
                                                                                                    cpu_percent,
                                                                                                    min_size,
                                                                                                    max_size),
                            shell=True)

def calculate_deployment_cost(deployment_name, starting_time, ending_time = None):

    # cmd = "cat ~/go/data | grep \'" + deployment_name + "\'  | awk {\'print $2\'}"
    # output = (subprocess.check_output(cmd, shell=True)).decode('utf-8')
    # output = output.split('\n')[:-1]
    # pods = list(set(output))

    pod_data = {}

    cpu_quota = get_deployment_cpu_quota(deployment_name)

    pods = get_all_pods_from_deployment(deployment_name=deployment_name, safe=True)

    pods = [str(pod) for pod in pods]

    for pod in pods:
        pod_data[pod] = calculate_pod_cost(pod, cpu_quota, starting_time, ending_time)

    total_cost = sum(pod_data.values())

    return total_cost

def calculate_pod_cost(pod_name, cpu_quota, starting_time, ending_time):
    cmd = "cat ~/go/data | grep \'{}\'".format(pod_name)
    output = (subprocess.check_output(cmd, shell=True)).decode('utf-8')
    output = output.split('\n')[:-1]
    output = [str(o) for o in output]

    cost = 0
    if output[-1].split(" ")[0] == "Delete":
        time_stopped = int(output[1].split(" ")[2])

        if time_stopped < starting_time:
            return 0

        cost = int(output[1].split(" ")[2]) - starting_time
    else:

        if not ending_time:
            ending_time = int(time.time())

        if output[0].split(" ")[0] == "Add" and int(output[0].split(" ")[2]) > starting_time:
            cost = ending_time - int(output[0].split(" ")[2])
        else:
            cost = ending_time - starting_time

    return cost * cpu_quota

def wait_for_autoscaler_steady_state(scale_deployment_name, workload_deployment_name=None, flag=None, selector=None,
                                     utilization = None, ab = True, return_queue = None):


    label_all_unlabeled_nodes_as_service()

    performance_data = []

    label = selector
    if not selector:
        label = scale_deployment_name

    pod_count_every_30_sec= []

    count = 0
    while count < 180:

        # if (count % 12 == 0):
        #     label_all_unlabeled_nodes_as_service()
        #     # print("done parsing results")
        #
        #
        if (count % 30 == 0):
        #
            current_pod_count = len(get_all_pods_from_deployment(deployment_name=scale_deployment_name, safe=True))
        #     # print("Current pod count is {}".format(current_pod_count))
            pod_count_every_30_sec.append(current_pod_count)
        #     # print(calculate_deployment_cost(scale_deployment_name, int(time.time())))
        #     if (count % 60 == 0):
        #         dct_to_add = {}
        #         dct_to_add["pods_count"] = current_pod_count
        #         if workload_name:
        #             dct_to_add["data"] = parse_results(workload_deployment_name, num_iterations=1, ab=ab)
        #         performance_data.append(dct_to_add)


        sleep(1)
        count += 1

        # print(count)


    while True:

        try:

            pod_count = len(get_all_pods_from_deployment(deployment_name=scale_deployment_name, safe=True))
            #
            if pod_count_every_30_sec[-6] == pod_count:
                print("Found Steady state")
                return_queue.put({scale_deployment_name: int(time.time())})
            #
            # if (count % 12 == 0):
            #     label_all_unlabeled_nodes_as_service()
            #
            if count % 30 == 0:
            #
                dict_to_add = {}
            #
            #     if workload_name:
            #         dict_to_add["data"] = parse_results(workload_deployment_name, num_iterations=1, ab=ab)
            #
                dict_to_add["pods_count"] = len(get_all_pods_from_deployment(scale_deployment_name, safe=True))
            #
            #     performance_data.append(dict_to_add)
            #
                pod_count_every_30_sec.append(dict_to_add['pods_count'])

                print(pod_count_every_30_sec)


            sleep(1)

            count += 1

            # print(count)

        except Exception as e:
            print(e.args)
            print(e.message)


def run_utilization_experiment_variable_workload(scale_deployment_list, workload_services_list, workload_deployment_name, workload_size_list, num_iterations,
                               min_scaleout, max_scaleout, cpu_cost_list, additional_args_dict, node_count=4, label="", ab = True):


    performance_data_list = defaultdict(list)
    performance_data_list_2 = defaultdict(list)
    cost_data_list  = defaultdict(list)
    cost_data_list_2 = defaultdict(list)

    performance_data_while_scaling = defaultdict(list)
    performance_data_while_scaling2 = defaultdict(list)

    for trial in range(1):

        for utilization in [20]:

            for index in range(len(scale_deployment_list)):
                deploying = True
                while deploying:
                    try:
                        create_scale_deployment(scale_deployment_list[index], cpu_cost=cpu_cost_list[index])
                        deploying = False
                    except:
                        pass

                wait_for_scale_deployment(scale_deployment_list[index])

            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    create_workload_deployment("{}-{}-{}".format(workload_deployment_name, workload_services_list[index], endpoint),
                                               workload_size_list[index],
                                               workload_services_list[index], endpoint, node_count=node_count)

            sleep(20)

            for index in range(len(scale_deployment_list)):
                create_autoscaler(scale_deployment_list[index], utilization, min_scaleout, max_scaleout)

            print("Creating autoscalers with utilization {}".format(utilization))

            print("Waiting for autoscaler metrics")

            times_of_deployment = []
            for index in range(len(scale_deployment_list)):
                times_of_deployment.append(wait_for_autoscale_metrics(scale_deployment_list[index]))

            print("Waiting for Autoscaler steady state")

            queue = mp.Queue()

            process_list = []
            for index in range(len(scale_deployment_list)):
                if scale_deployment_list[index] in workload_services_list:
                    args = [scale_deployment_list[index], "{}-{}".format(workload_deployment_name, scale_deployment_list[index]),
                            False, None, utilization, queue]
                else:
                    args = [scale_deployment_list[index], None, False, None, utilization, queue]
                process_list.append(mp.Process(target = wait_for_autoscaler_steady_state, args = args))
                process_list[-1].start()

            for process in process_list:
                process.join()
            # Collects and writes cost data


            scale_name_to_ending_time = {}
            for process in process_list:
                queue_result = queue.get()
                scale_name_to_ending_time[queue_result.keys()[0]] = queue_result.values()[0]

            for index in scale_deployment_list:


                cost_data = calculate_deployment_cost(scale_deployment_list[index], times_of_deployment[index],
                    scale_name_to_ending_time[scale_deployment_list[index]])


                print("Current Cpu Cost: {}".format(cost_data))

                print("Writing cost data to file")

                with open('cost_results_{}_{}_{}'.format(scale_deployment_list[index], label, trial), 'w') as file:

                    cost_data_list[scale_deployment_list[index]].append(({"data": cost_data, "utilization": utilization}))

                    file.write(json.dumps(cost_data_list))


            # Collects and writes performance data

            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    pods_data = parse_results("{}-{}-{}".format(workload_deployment_name, workload_services_list[index], endpoint),
                                              num_iterations=num_iterations, ab=ab)


                    print("Writing performance data to file")


                    performance_data_list[workload_services_list[index]].append({"data": pods_data,
                                                                                 "utilization": utilization,
                                                                                 "endpoint" : endpoint})



                with open('performance_results_{}_{}_{}'.format(workload_services_list[index], label, trial), 'w') as file:

                    file.write(json.dumps(performance_data_list[workload_services_list[index]]))

            #################################################################################

            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    scale_workload_deployment("{}-{}-{}".format(workload_deployment_name, workload_services_list[index],
                                                                endpoint), workload_size_list[index] * 6)

            sleep(30)

            timestamp2 = int(time.time())

            print("Waiting for Autoscaler steady state")

            queue = mp.Queue()

            process_list = []
            for index in range(len(scale_deployment_list)):
                if scale_deployment_list[index] in workload_services_list:
                    args = [scale_deployment_list[index],
                            "{}-{}".format(workload_deployment_name, scale_deployment_list[index]),
                            False, None, utilization, queue]
                else:
                    args = [scale_deployment_list[index], None, False, None, utilization, queue]
                process_list.append(mp.Process(target=wait_for_autoscaler_steady_state, args=args))
                process_list[-1].start()

            for process in process_list:
                process.join()
            # Collects and writes cost data


            scale_name_to_ending_time = {}
            for process in process_list:
                queue_result = queue.get()
                scale_name_to_ending_time[queue_result.keys()[0]] = queue_result.values()[0]

            for index in scale_deployment_list:


                cost_data = calculate_deployment_cost(scale_deployment_list[index], times_of_deployment[index],
                    scale_name_to_ending_time[scale_deployment_list[index]])


                print("Current Cpu Cost: {}".format(cost_data))

                print("Writing cost data to file")

                with open('cost_results2_{}_{}_{}'.format(scale_deployment_list[index], label, trial), 'w') as file:

                    cost_data_list_2[scale_deployment_list[index]].append(({"data": cost_data, "utilization": utilization}))

                    file.write(json.dumps(cost_data_list_2))


            # Collects and writes performance data
            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    pods_data = parse_results("{}-{}-{}".format(workload_deployment_name, workload_services_list[index], endpoint),
                                              num_iterations=num_iterations, ab=ab)


                    print("Writing performance data to file")


                    performance_data_list_2[workload_services_list[index]].append({"data": pods_data,
                                                                                 "utilization": utilization,
                                                                                 "endpoint" : endpoint})



                with open('performance_results2_{}_{}_{}'.format(workload_services_list[index], label, trial), 'w') as file:

                    file.write(json.dumps(performance_data_list_2[workload_services_list[index]]))



            # file = open('performance_results_while_scaling_{}_{}'.format(utilization, label), 'w')
            # file.write(json.dumps(performance_data_while_scaling[utilization]))
            # file.flush()
            # file.close()
            #
            # file = open('performance_results_while_scaling2_{}_{}'.format(utilization, label), 'w')
            # file.write(json.dumps(performance_data_while_scaling2[utilization]))
            # file.flush()
            # file.close()

            for service in scale_deployment_list:

                subprocess.Popen(['kubectl', 'delete', 'hpa/{}'.format(service)])

                subprocess.Popen(['kubectl', 'delete', 'deployment', service])


            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    subprocess.Popen(['kubectl', 'delete', 'deployment', "{}-{}-{}"
                                     .format(workload_deployment_name, workload_services_list[index], endpoint)])


            sleep(25)

def wait_for_scale_deployment(deployment_name):

    ready = False
    while not ready:
        output = subprocess.check_output("kubectl get pods | grep \'" + deployment_name + "\' | awk {\'print $3\'}", shell=True) \
            .decode('utf-8').split("\n")[0]
        if output == "Running":
            ready = True
        sleep(2)

def wait_for_autoscale_metrics(deployment_name):
    ready = False
    try:
        while not ready:
            output = subprocess.check_output("kubectl describe hpa/{}".format(deployment_name) + "| grep \"<unknown>\""
                                             ,shell=True).decode('utf-8')
            sleep(5)
    except:
        return int(time.time())



if __name__ == "__main__":

    scale_deployment_list = ["node-app"]

    workload_name = "workload-manager"

    workload_services_list = ["node-app"]

    endpoints = {}

    service_name = "node-app"

    additional_args = ""

    nodes_capacity = get_node_capacity()



    pods_per_node = 4.4

    cpu_quota = None

    delete = False
    try:
        subprocess.Popen(['kubectl', 'delete', 'deployment', scale_name])
        delete = True
    except:
        pass

    try:
        subprocess.Popen(['kubectl', 'delete', 'deployment', workload_name])
        delete = True
    except:
        pass

    try:
        subprocess.Popen(['kubectl', 'delete', 'hpa/{}'.format(scale_name)])
        delete = True
    except:
        pass

    if delete:
        sleep(20)

    # try:
    #     subprocess.Popen(['kubectl', 'create', '-f', 'manifests/{}.yaml'.format(scale_name)])
    # except:
    #     pass
    #
    # workload_size = 12
    #
    # create_autoscaler(scale_name, 10, 10, 200)
    # create_workload_deployment(workload_name, workload_size, service_name)
    # wait_for_pods_to_be_deployed(workload_name, workload_size)
    # wait_for_autoscale_metrics(scale_name)
    # current_time = int(time.time())
    #
    #
    #
    # while True:
    #     print("Current cpu cost: {}".format(calculate_deployment_cost(scale_name, current_time)))
    #     sleep(10)
    #
    #

    run_utilization_experiment_variable_workload(scale_deployment_name=scale_name,
                                                 workload_deployment_name=workload_name,
                                                 service_name=service_name,
                                                 additional_args=additional_args,
                                                 workload_size = 3,
                                                 num_iterations=20,
                                                 min_scaleout=10,
                                                 max_scaleout=500,
                                                 cpu_cost=cpu_quota if cpu_quota else str(get_node_capacity() / float(pods_per_node)),
                                                 label="{}podsPerNode".format(pods_per_node),
                                                 workload_node_count=9,
                                                 ab=True)

    # parse_results(workload_name, 2)
    # wait_for_autoscaler_steady_state(scale_name, workload_name)


    # print(calculate_deployment_cost(scale_name, int(time.time())))