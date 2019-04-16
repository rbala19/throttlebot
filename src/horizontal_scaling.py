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
import inspect

def create_autoscaler(deployment_name, cpu_percent, min_size, max_size):

    output = subprocess.check_output("kubectl autoscale deployment {} --cpu-percent={} --min={} --max={}".format(deployment_name,
                                                                                                    cpu_percent,
                                                                                                    min_size,
                                                                                                    max_size),
                            shell=True)


def create_experiment_folder(scale_deployment_list, workload_services_list, workload_deployment_name, workload_size_list,
                                                 increased_workload_size_list, num_iterations,
                               min_scaleout, max_scaleout, cpu_cost_list, additional_args_dict, workload_node_count=4, label="", ab = True,
                                                 namespace="default"):
    experiment_start_time = str(time.ctime())
    # experiment_start_time = experiment_start_time.replace(":", "-")
    # experiment_start_time = experiment_start_time.strip(" ")
    directory = './autoscaling_results/{}'.format(experiment_start_time)

    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' + directory)

    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    with open(directory + '/experiment_settings.txt', 'a') as experiment_setting_file:
        for i in args:
            experiment_setting_file.write('{} = {}'.format(i, values[i]))

    print("Created experiment directory")

    return directory

def calculate_deployment_cost(deployment_name, starting_time, namespace, ending_time = None):

    pod_data = {}

    cpu_quota = get_deployment_cpu_quota(deployment_name)

    pods = get_all_pods_from_deployment(deployment_name=deployment_name, safe=True, namespace=namespace)

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

def wait_for_autoscaler_steady_state(scale_deployment_name, namespace, return_queue = None):


    # label_all_unlabeled_nodes_as_service()

    pod_count_every_30_sec= []

    count = 0
    while count < 180:


        if (count % 30 == 0):

            pod_count_every_30_sec.append(len(get_all_pods_from_deployment(deployment_name=scale_deployment_name, safe=True,
                                                                           namespace=namespace)))
            print("{}: {}".format(scale_deployment_name, pod_count_every_30_sec))

        sleep(1)
        count += 1



    while True:

        try:

            pod_count = len(get_all_pods_from_deployment(deployment_name=scale_deployment_name, safe=True, namespace=namespace))

            if pod_count_every_30_sec[-6] == pod_count:
                print("Found Steady state")
                return_queue.put({scale_deployment_name: int(time.time())})
                return

            if count % 30 == 0:

                pod_count_every_30_sec.append(len(get_all_pods_from_deployment(scale_deployment_name, safe=True, namespace=namespace)))

                print("{}: {}".format(scale_deployment_name, pod_count_every_30_sec))


            sleep(1)

            count += 1


        except Exception as e:
            print(e.args)
            print(e.message)

def generate_workload_name(workload_prefix, service_name, endpoint):
    str_to_return = "{}-{}".format(workload_prefix, service_name)

    if endpoint:
        str_to_return += "-{}".format(endpoint.replace("/", "-"))

    return str_to_return


def run_utilization_experiment_variable_workload(scale_deployment_list, workload_services_list, workload_deployment_name, workload_size_list,
                                                 increased_workload_size_list, num_iterations,
                               min_scaleout, max_scaleout, cpu_cost_list, additional_args_dict, workload_node_count=4, label="", ab = True,
                                                 namespace="default", experiment_dir = "./"):


    performance_data_list = defaultdict(list)
    performance_data_list_2 = defaultdict(list)
    cost_data_list  = defaultdict(list)
    cost_data_list_2 = defaultdict(list)
    time_to_steady = defaultdict(list)
    time_to_steady2 = defaultdict(list)

    for trial in range(1):

        for utilization in [20]:

            assign_nodes(node_count=workload_node_count)

            #Deploying all scale deployments

            for index in range(len(scale_deployment_list)):
                deploying = True
                while deploying:
                    try:
                        # print("Deploying {}".format(scale_deployment_list[index]))
                        create_scale_deployment(scale_deployment_list[index], cpu_cost=cpu_cost_list[index])
                        deploying = False
                    except:
                        traceback.print_exc()
                        pass


            sleep(10)

            #Deploying all workload deployments per service per endpoint

            print("Deploying workload pods")

            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    create_workload_deployment(generate_workload_name(workload_deployment_name, workload_services_list[index], endpoint),
                                               workload_size_list[index],
                                               workload_services_list[index], endpoint, node_count=workload_node_count)

            sleep(10)

            #Create autoscaler for all scale deployments


            print("Creating autoscalers with utilization {}".format(utilization))

            for index in range(len(scale_deployment_list)):
                create_autoscaler(scale_deployment_list[index], utilization, min_scaleout, max_scaleout)



            print("Waiting for autoscaler metrics")


            queue = mp.Queue()

            process_list = []
            for index in range(len(scale_deployment_list)):
                args = [scale_deployment_list[index], queue]

                process_list.append(mp.Process(target=wait_for_autoscale_metrics, args=args))
                process_list[-1].start()

            for process in process_list:
                process.join()


            times_of_deployment = {}
            for process in process_list:
                queue_result = queue.get()
                times_of_deployment[list(queue_result.keys())[0]] = list(queue_result.values())[0]


            print("Waiting for Autoscaler steady state")


            #Wait for steady state for all scale deployments

            queue = mp.Queue()

            process_list = []
            for index in range(len(scale_deployment_list)):
                args = [scale_deployment_list[index], namespace, queue]

                process_list.append(mp.Process(target=wait_for_autoscaler_steady_state, args=args))
                process_list[-1].start()

            scale_name_to_ending_time = {}
            for process in process_list:
                queue_result = queue.get()
                scale_name_to_ending_time[list(queue_result.keys())[0]] = list(queue_result.values())[0]


            # times_of_deployment = {}
            # times_of_deployment["apartmentapp"] = int(time.time())
            # times_of_deployment["elasticsearch"] = int(time.time())
            # times_of_deployment["kibana"] = int(time.time())
            #
            # sleep(60)
            #
            #
            # scale_name_to_ending_time = {}
            # scale_name_to_ending_time["apartmentapp"] = int(time.time())
            # scale_name_to_ending_time["elasticsearch"] = int(time.time())
            # scale_name_to_ending_time["kibana"] = int(time.time())

            #Logging time to steady state

            for index in range(len(scale_deployment_list)):
                with open('{}/time_to_steady_{}_{}_{}'.format(experiment_dir, scale_deployment_list[index], label, trial), 'w') as file:

                    time_to_steady[scale_deployment_list[index]].append(scale_name_to_ending_time[scale_deployment_list[index]] -
                                                                        times_of_deployment[scale_deployment_list[index]])

                    file.write(json.dumps(time_to_steady[scale_deployment_list[index]]))


            for index in range(len(scale_deployment_list)):


                cost_data = calculate_deployment_cost(deployment_name=scale_deployment_list[index],
                                                      starting_time=times_of_deployment[scale_deployment_list[index]],
                                                      namespace=namespace,
                    ending_time=scale_name_to_ending_time[scale_deployment_list[index]])


                print("Current Cpu Cost: {}".format(cost_data))

                print("Writing cost data to file")

                with open('{}/cost_results_{}_{}_{}'.format(experiment_dir, scale_deployment_list[index], label, trial), 'w') as file:

                    cost_data_list[scale_deployment_list[index]].append(({"data": cost_data, "utilization": utilization}))

                    file.write(json.dumps(cost_data_list[scale_deployment_list[index]]))


            # Collects and writes performance data

            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    pods_data = parse_results(generate_workload_name(workload_deployment_name, workload_services_list[index], endpoint),
                                              num_iterations=num_iterations, ab=ab, namespace=namespace)


                    print("Writing performance data to file")


                    performance_data_list[workload_services_list[index]].append({"data": pods_data,
                                                                                 "utilization": utilization,
                                                                                 "endpoint" : endpoint})



                with open('{}/performance_results_{}_{}_{}'.format(experiment_dir, workload_services_list[index], label, trial), 'w') as file:

                    file.write(json.dumps(performance_data_list[workload_services_list[index]]))

            #################################################################################



            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    scale_workload_deployment(generate_workload_name(workload_deployment_name, workload_services_list[index],
                                                                endpoint), increased_workload_size_list[index])

            sleep(30)

            timestamp2 = int(time.time())

            print("Waiting for Autoscaler steady state")

            queue = mp.Queue()

            process_list = []
            for index in range(len(scale_deployment_list)):

                args = [scale_deployment_list[index], namespace, queue]

                process_list.append(mp.Process(target=wait_for_autoscaler_steady_state, args=args))
                process_list[-1].start()

            for process in process_list:
                process.join()
            # Collects and writes cost data


            scale_name_to_ending_time = {}
            for process in process_list:
                queue_result = queue.get()
                scale_name_to_ending_time[list(queue_result.keys())[0]] = list(queue_result.values())[0]

            #Logging time to steady state

            for index in range(len(scale_deployment_list)):
                with open('{}/time_to_steady2_{}_{}_{}'.format(experiment_dir, scale_deployment_list[index], label, trial), 'w') as file:

                    time_to_steady2[scale_deployment_list[index]].append(scale_name_to_ending_time[scale_deployment_list[index]] -
                                                                        timestamp2)

                    file.write(json.dumps(time_to_steady2[scale_deployment_list[index]]))



            for index in range(len(scale_deployment_list)):


                cost_data = calculate_deployment_cost(deployment_name=scale_deployment_list[index],
                                                      starting_time=timestamp2,
                                                      namespace=namespace,
                    ending_time=scale_name_to_ending_time[scale_deployment_list[index]])


                print("Current Cpu Cost: {}".format(cost_data))

                print("Writing cost data to file")

                with open('{}/cost_results2_{}_{}_{}'.format(experiment_dir, scale_deployment_list[index], label, trial), 'w') as file:

                    cost_data_list_2[scale_deployment_list[index]].append(({"data": cost_data, "utilization": utilization}))

                    file.write(json.dumps(cost_data_list_2[scale_deployment_list[index]]))


            # Collects and writes performance data
            for index in range(len(workload_services_list)):

                for endpoint in additional_args_dict[workload_services_list[index]]:

                    pods_data = parse_results(generate_workload_name(workload_deployment_name, workload_services_list[index], endpoint),
                                              num_iterations=num_iterations, ab=ab, namespace=namespace)


                    print("Writing performance data to file")


                    performance_data_list_2[workload_services_list[index]].append({"data": pods_data,
                                                                                 "utilization": utilization,
                                                                                 "endpoint" : endpoint})



                with open('{}/performance_results2_{}_{}_{}'.format(experiment_dir, workload_services_list[index], label, trial), 'w') as file:

                    file.write(json.dumps(performance_data_list_2[workload_services_list[index]]))


            delete_all_deployments(scale_deployment_list, workload_services_list, additional_args_dict)

def wait_for_scale_deployment(deployment_name, namespace="default"):

    ready = False
    while not ready:
        output = subprocess.check_output("kubectl get pods --namespace=" + namespace +
                                         "| grep \'" + deployment_name + "\' | awk {\'print $3\'}", shell=True) \
            .decode('utf-8').split("\n")[0]
        if output == "Running":
            ready = True
        sleep(2)

def wait_for_autoscale_metrics(deployment_name, queue):
    ready = False
    try:
        while not ready:
            output = subprocess.check_output("kubectl describe hpa {}".format(deployment_name) + "| grep \"<unknown>\""
                                             ,shell=True).decode('utf-8')
            sleep(5)
    except:
        queue.put({deployment_name: int(time.time())})
        return

def delete_all_deployments(scale_deployment_list, workload_services_list, additional_args_dict):
    delete = False
    for scale_name in scale_deployment_list:
        try:
            subprocess.Popen(['kubectl', 'delete', 'deployment', scale_name])
            delete = True
            subprocess.Popen(['kubectl', 'delete', 'hpa', scale_name])
        except:
            pass

    for index in range(len(workload_services_list)):

        for endpoint in additional_args_dict[workload_services_list[index]]:

            try:

                subprocess.check_output('kubectl delete deployment {}'.format(generate_workload_name(
                    workload_name, workload_services_list[index], endpoint)), shell=True)
                delete = True
            except:
                pass

    if delete:
        sleep(20)

if __name__ == "__main__":

    scale_deployment_list = ["apartmentapp", "elasticsearch", "kibana"]
    pods_per_nodes_list = [4.4, 4.4, 4.4]
    workload_services_list = ["apartmentapp"]

    additional_args_dict = defaultdict(list)
    additional_args_dict["apartmentapp"].extend(['app/psql/users', 'app/mysql/users', 'app/users', 'app/elastic/users/3'])

    workload_name = "workload-manager"
    workload_size_list = [2]
    increased_workload_size_list = [10]

    nodes_capacity = get_node_capacity()
    num_iterations = 20
    min_scaleout = 10
    max_scaleout = 500
    cpu_quota = None
    workload_node_count = 22
    ab = True
    namespace="default"


    delete_all_deployments(scale_deployment_list, workload_services_list, additional_args_dict)


    experiment_dir = create_experiment_folder(scale_deployment_list=scale_deployment_list,
                                                 workload_deployment_name=workload_name,
                                                 workload_services_list=workload_services_list,
                                                 additional_args_dict=additional_args_dict,
                                                 workload_size_list=workload_size_list,
                                                 increased_workload_size_list=increased_workload_size_list,
                                                 num_iterations=num_iterations,
                                                 min_scaleout=min_scaleout,
                                                 max_scaleout=max_scaleout,
                                                 cpu_cost_list=[str(get_node_capacity() / float(pods_per_node)) for pods_per_node in pods_per_nodes_list],
                                                 label="{}podsPerNode".format(pods_per_nodes_list[0]),
                                                 workload_node_count=20,
                                                 ab=True,
                                                 namespace=namespace)

    run_utilization_experiment_variable_workload(scale_deployment_list=scale_deployment_list,
                                                 workload_deployment_name=workload_name,
                                                 workload_services_list=workload_services_list,
                                                 additional_args_dict=additional_args_dict,
                                                 workload_size_list=workload_size_list,
                                                 increased_workload_size_list=increased_workload_size_list,
                                                 num_iterations=num_iterations,
                                                 min_scaleout=min_scaleout,
                                                 max_scaleout=max_scaleout,
                                                 cpu_cost_list=[str(get_node_capacity() / float(pods_per_node)) for pods_per_node in pods_per_nodes_list],
                                                 label="{}podsPerNode".format(pods_per_nodes_list[0]),
                                                 workload_node_count=20,
                                                 ab=True,
                                                 namespace=namespace,
                                                 experiment_dir=experiment_dir)

