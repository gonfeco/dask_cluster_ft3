import argparse
import subprocess
import os

def look_in_environment(environment_variable):
    """
    Esta funcion intentan evaluar la variable de entorno que se le pasa.
    Inputs:
        environment_variable: Variable de entorno de la que se quiere saber el valor. Si no la hay levanta un error.
    Outputs:
        value_variable: Valor de la variable de entorno si se consigue leer correctamente.
    """
    try:
        value_variable = os.environ[environment_variable]
        #print flag
    except KeyError:
        print("Not "+environment_variable+" environment variable!!")
        raise KeyError
    return value_variable

def get_nodes_ips():
    #Get Nodes
    nodes = look_in_environment("TFSERVER")
    #print(nodes)
    nodes_ips = nodes.split('\n')
    #print(NodesIPs)
    return nodes_ips

if __name__ == "__main__":
    FLAGS = None
    parser = argparse.ArgumentParser()
    parser.add_argument('-local', default='/tmp', help='Local Storage')
    FLAGS, unparsed = parser.parse_known_args()


    nodes_ip = get_nodes_ips()
    print('nodes_ip: {} '.format(nodes_ip))
    scheduler_ip = nodes_ip[0]
    print('scheduler_ip: {}'.format(scheduler_ip))

    #What number is my node inside of the list of nodes
    node_id = int(look_in_environment("SLURM_NODEID"))
    print('node_id: {}'.format(node_id))
    #Inside on the node what number I am
    local_id = int(look_in_environment("SLURM_LOCALID"))
    print('local_id: {}'.format(local_id))
    #Creating unique identifier for each task
    final_id = local_id + node_id
    print('final_id: {}'.format(final_id))
    
   # tira_ssh = 'ssh -t -L 8787:localhost:8787 {}@ft3.cesga.es ssh -L 8787:{}:8787'.format(
   #     os.getlogin(), scheduler_ip)

    if final_id == 0:
        dask_scheduler = 'dask-scheduler --host {} --port 8786 --dashboard'.format(
            scheduler_ip)
        print('Command line to create Scheduler: {}'.format(dask_scheduler))
        #Lanzamos el comado que monta el Scheduler
        process = subprocess.run(dask_scheduler.split(), stdout=subprocess.PIPE)
    else:
        #Creamos el comando que va a lanzar el Worker. Necesitamos la IP del Scheduler
        worker = 'dask-worker {} --no-nanny --nthreads 1 --local-directory {}'.format(
            'tcp://'+scheduler_ip+':8786', FLAGS.local)
        print('Command line to create worker: {}'.format(worker))
        #Lanzamos el comando que levanta el Worker
        process = subprocess.run(worker.split(), stdout=subprocess.PIPE)
