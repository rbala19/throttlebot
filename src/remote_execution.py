import paramiko
import logging

def get_client(ip, orchestrator='quilt'):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if orchestrator == 'quilt':
        client.connect(ip, username='quilt', password="")
    else:
        client.connect(ip, username="admin", password="")
    return client

def ssh_exec(ssh_client, cmd, modifies_container=False, return_error=False):
    _,stdout,stderr = ssh_client.exec_command(cmd)
    err = stderr.read()
    if err:
        logging.info("Error execing {}: {}".format(cmd, err))

        if modifies_container:
            raise SystemError('Error caused by container ID')
    
    if return_error:
        return _,stdout,stderr

def close_client(ssh_client):
    ssh_client.close()
