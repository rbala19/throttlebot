import argparse
import csv
import ConfigParser
import time
import numpy as np

import redis.client
import subprocess
import os
import sys
currentDirectory = os.getcwd()
sys.path.append("/Users/rahulbalakrishnan/Desktop/throttlebot/src")
import modify_resources as resource_modifier
import run_experiment
import filter_policy, instance_specs
import remote_execution as re
from mr import MR
import datetime
from collections import defaultdict


def ip_to_service_list(service_names):

    output = str(subprocess.check_output("quilt ps | grep \'Amazon\' | awk {\'print $6\'}", shell=True).decode("utf-8"))
    output = output.split('\n')
    ips = output[:-1]
    # print(ips)

    output = str(subprocess.check_output("quilt ps | grep \'Amazon\' | awk {\'print $1\'}", shell=True).decode("utf-8"))
    output = output.split('\n')
    machines = output[:-1]
    # print(machines)

    machine_to_ip = {}
    for index in range(len(machines)):
        machine_to_ip[machines[index]] = ips[index]

    output = str(
        subprocess.check_output("quilt ps | grep \'running\' | awk {\'print $3\'}", shell=True).decode("utf-8"))
    output = output.split('\n')
    services = output[:-1]
    # print(services)

    output = str(
        subprocess.check_output("quilt ps | grep \'running\' | awk {\'print $2\'}", shell=True).decode("utf-8"))
    output = output.split('\n')
    machines = output[:-1]
    # print(machines)

    output = str(
        subprocess.check_output("quilt ps | grep \'running\' | awk {\'print $1\'}", shell=True).decode("utf-8"))
    output = output.split('\n')
    containers = output[:-1]

    machines_to_services = defaultdict(list)

    for index in range(len(machines)):
        machine = machines[index]
        service_name = services[index]
        services_matched = []
        for service in service_names:

            if service in service_name:
                services_matched.append(service)

        if not services_matched:
            # print(service_name)
            continue
        earliest_service = min(services_matched, key=service_name.index)
        machines_to_services[machine].append(earliest_service)


    ip_to_services = {}
    for machine in machines_to_services:
        key_name = machine_to_ip[machine]
        value = machines_to_services[machine]
        ip_to_services[key_name] = list(set(value))

    print(ip_to_services)
    return ip_to_services


def master_node():
    mn = str(subprocess.check_output("quilt ps | grep Master | awk {\'print $6\'}", shell=True).decode("utf-8"))[:-1]
    return mn

def load_config():
    d = {}
    with open("/Users/rahulbalakrishnan/Desktop/throttlebot/src/spearmint/bayOptSearch/workload_config") as f:
        for line in f:
            try:
               (key, val) = line.split(" = ")
               d[key] = val
            except:
                key = line.split(" =")[0]
                d[key] = None
                pass

    return d

workload_config = load_config()

past_results = {}


def main(job_id, params):


    # return 1
    return explore_spearmint(workload_config, params)




def explore_spearmint(workload_config, params):

    # return np.random.uniform(0, 15)

    redis_db = redis.StrictRedis(host='0.0.0.0', port=6379, db=0)



    t0 = time.time()

    print("The paramters are {}".format(params))
    # Set all fields using params object


    machine_list = [[10, 9, 8], [0, 5], [6, 8], [4, 5], [7, 0], [2, 8, 1], [6, 7, 1],
                    [0, 3, 7], [5, 6]]

    #hardcoded checks
    resource_types = ["CPU-QUOTA", "MEMORY", "NET", "DISK"]
    #Machine 1
    for type in resource_types:
        benchmark = 120
        for item in machine_list:
            sum = 0
            for index in item:
                sum += params[type][index]
            if sum > benchmark:
                print("Type is {} and machine is {}".format(type, item))
                return 1/0




    workload_config["type"] = "hotrod"
    workload_config["workload_num"] = 1
    masterNode = ["54.215.210.18"]
    experiment_trials = int(workload_config['num_trials']) if 'num_trials' in workload_config else 5
    # service_names = ["node-app", "haproxy", "mongo"]
    service_names =["hantaowang/hotrod-mapper:log","haproxy:1.7","hantaowang/hotrod-mapper:log","postgres:9.4","hantaowang/hotrod-seed","hantaowang/hotrod-customer:log","hantaowang/hotrod-route:log","hantaowang/hotrod-api:log","hantaowang/hotrod-driver:log","hantaowang/hotrod-frontend","nginx:1.7.9","hantaowang/redis"]
    dct = ip_to_service_list(service_names)
    for ip in dct:
        if "haproxy" in dct[ip]:
            workload_config["frontend"] = [ip]
            break

    # service_index_dct = {"node-app": 0, "haproxy": 1, "mongo": 2}
    service_index_dct = {"hantaowang/hotrod-mapper:log": 0,"haproxy:1.7": 1, "hantaowang/redis": 2,
                         "postgres:9.4": 3,
                         "hantaowang/hotrod-seed": 4,"hantaowang/hotrod-customer:log": 5,
                         "hantaowang/hotrod-route:log": 6,"hantaowang/hotrod-api:log": 7,
                         "hantaowang/hotrod-driver:log": 8,"hantaowang/hotrod-frontend": 9,
                         "nginx:1.7.9": 10}

    # params["CPU-QUOTA"] = [40, 40, 40]
    # params["DISK"] = []
    # params["MEMORY"] = 40
    # params["NET"] = 40

    # for mr in params:
    #
    #     for machine in dct:
    #         sum = 0
    #         for service in dct[machine]:
    #
    #            client = re.get_client(machine)
    #            _, containers, _ = client.exec_command("docker ps | grep " + service + " | awk {'print $1'}")
    #            containers = containers.read().split("\n")
    #            if len(containers) > 1:
    #                containers = containers[:-1]
    #            sum += params[mr][service_index_dct[service]] * len(containers)
    #
    #         threshold = 120
    #         cpu_threshold = 200
    #         toCompare = cpu_threshold if mr == "CPU-QUOTA" else threshold
    #         if sum > toCompare:
    #             print("Sum for {} is {}, which exceeds {}".format(mr, sum, toCompare))
    #             return 1/0

    for ip in dct:
        workload_config["request_generator"] = [ip]
        client = re.get_client(workload_config["request_generator"][0])

        for name in dct[ip]:

            _, containers, _ = client.exec_command("docker ps | grep " + name + " | awk {'print $1'}")
            containers = containers.read().split("\n")
            if len(containers) > 1:
                containers = containers[:-1]

            print("{} for {} in ip {}".format(containers, name, ip))

            instances = [(str(workload_config["request_generator"][0]), container) for container in containers]


            for container in containers:
                resource_modifier.set_cpu_quota(client, container, 250000, instance_specs.get_instance_specs(workload_config["machine_type"])["CPU-CORE"] * params["CPU-QUOTA"][service_index_dct[name]])



            for mr in params:
                if mr not in ["CPU-QUOTA", "CPU-CORE"]:
                    temp_mr = MR(name, mr, instances)

                    max_capacity = instance_specs.get_instance_specs(workload_config["machine_type"])[mr]


                    temp = (params[mr][service_index_dct[name]]/ 100.0) * max_capacity

                    resource_modifier.set_mr_provision(temp_mr, temp,
                                                       workload_config, redis_db)



    workload_config["request_generator"] = masterNode

    # client = re.get_client(masterNode[0])
    # re.ssh_exec(client, "sudo apt install apache2-utils")
    # re.ssh_exec(client, "curl -O https://raw.githubusercontent.com/TsaiAnson/mean-a/master/Master\%20Node\%20Files/clear_entries.py")

    # re.ssh_exec(client, "curl -O https://raw.githubusercontent.com/TsaiAnson/mean-a/master/Master\%20Node\%20Files/post.json")


    print("Using {} trials".format(experiment_trials))
    experiment_results = run_experiment.measure_runtime(workload_config, experiment_trials)

    # Write latency values to a csv, take the current time and then
    # subtract it from the time that the spearmint_runner was initiated
    with open('/Users/rahulbalakrishnan/Desktop/throttlebot/src/spearmint_results.csv','a') as csvfile:
        field_names = ['time', "latency_50", "latency_90", "latency_99", "performance"]
        for trial in range(len(experiment_results['latency_99'])):
            result_dict = {}
            # To determine time elapsed, we will record the time at the start of the experiment
            result_dict['time'] = datetime.datetime.now()
            result_dict['latency_50'] = filter_policy.mean_list(experiment_results['latency_50'])
            result_dict['latency_90'] = filter_policy.mean_list(experiment_results['latency_90'])
            result_dict['latency_99'] = filter_policy.mean_list(experiment_results['latency_99'])
            result_dict['performance'] = experiment_results['latency_99']
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writerow(result_dict)

    print("Experiment results are {}".format(experiment_results))
    mean_result = filter_policy.mean_list(experiment_results['latency_99'])
    std_result = np.std(np.array(experiment_results['latency_99']))

    return mean_result












