import subprocess
import time
import json

output = subprocess.check_output("grep \'Beat Time-to-beat with these stats\' best_results | tail -n 1",
                             shell=True)
# output = ps.communicate()
# ps2 = subprocess.Popen(shlex.split("tail -n 1"), stdin = ps.stdout, shell=False)


# output = "test"
output = str(output.decode("utf-8"))[:-1]

print(output[output.index(": ") + 2:])
data = json.loads(output[output.index(": ") + 2:])
print(data)