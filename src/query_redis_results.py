import redis.client
import json


redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

def read_summary_redis(redis_db, experiment_iteration_count, trial_count):
    hash_name = '{}summary'.format(experiment_iteration_count)

    dct = {}
    dct["current_perf"] = redis_db.hget(hash_name, 'current_perf')
    dct["elapsed_time"] = redis_db.hget(hash_name, 'elapsed_time')
    dct["analytic_perf"] = redis_db.hget(hash_name, 'analytic_perf')
    dct["current_std"] = redis_db.hget(hash_name, 'current_std')

    dct["trial_results"] = []
    for trial in range(trial_count):
        dct["trial_results"].append(redis_db.hget(hash_name, "trial{}_perf".format(trial)))

    return dct

result_to_write = {}
iter_count = 0
while True:
    dict_result = read_summary_redis(redis_db, iter_count, 5)
    if dict_result["current_perf"] == None:
        break
    else:
        result_to_write[iter_count] = dict_result
    iter_count += 1


with open("redis_dump", "w") as f:
    f.write(json.dumps(result_to_write))


