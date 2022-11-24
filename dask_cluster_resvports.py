import argparse
import subprocess
import os
import time
import json

def nodeset_expand(tira):
    if '-' in tira:
        lista = tira.split('-')
        if len(lista) != 2:
            raise ValueError("Mal")
        rango = list(range(int(lista[0]), int(lista[1]) + 1))
    else:
        rango = [tira]
    return rango

def nodeset_like(tira):
    node_name = tira.split('-')[0] + '-'
    resto = tira[tira.find('-') + 1:]
    if '[' in resto:
        resto = resto.replace('[', '')
        resto = resto.replace(']', '')
    nodes_ids = resto.split(',')
    list_of_nodes = []
    for i in nodes_ids:
        list_of_nodes = list_of_nodes + nodeset_expand(i)
    list_of_nodes = [node_name + str(i) for i in list_of_nodes]
    return list_of_nodes

def nodeset_like2(tira):
    resto = tira
    if '[' in resto:
        resto = resto.replace('[', '')
        resto = resto.replace(']', '')
    nodes_ids = resto.split(',')
    list_of_nodes = []
    for i in nodes_ids:
        list_of_nodes = list_of_nodes + nodeset_expand(i)
    list_of_nodes = [str(i) for i in list_of_nodes]
    return list_of_nodes

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

def launch_scheduler():
    ports = look_in_environment('SLURM_STEP_RESV_PORTS')
    port_list = nodeset_like2(ports)
    local_id = int(look_in_environment("SLURM_LOCALID"))
    print(port_list[local_id])
    dask_scheduler = 'dask-scheduler --port {} --dashboard --interface ib0 --scheduler-file {}'.format(
        port_list[local_id], './scheduler_info.json')
    print('Command line to create Scheduler: {}'.format(dask_scheduler))
    #Lanzamos el comado que monta el Scheduler
    process = subprocess.run(dask_scheduler.split(), stdout=subprocess.PIPE)
    return process

def launch_worker(scheduler_file=None, scheduler_address=None, local_folder='/tmp'):
    ports = look_in_environment('SLURM_STEP_RESV_PORTS')
    port_list = nodeset_like2(ports)
    local_id = int(look_in_environment("SLURM_LOCALID"))
    print(port_list[local_id])
    if scheduler_file is not None:
        worker = 'dask-worker --scheduler-file {} --worker-port {} --interface ib0 --no-nanny --nthreads 1 --local-directory {}'.format(
            scheduler_file, port_list[local_id], local_folder)
    elif scheduler_address is not None:
        worker = 'dask-worker {} --worker-port {} --interface ib0 --no-nanny --nthreads 1 --local-directory {}'.format(
            scheduler_address, port_list[local_id], local_folder)
    else:
        raise ValueError('No tengo direccion del scheduler')
    print('Command line to create worker: {}'.format(worker))
    #Lanzamos el comando que levanta el Worker
    process = subprocess.run(worker.split(), stdout=subprocess.PIPE)
    return process

def test_scheduler_file(json_file_name):
    isfile = False
    counter = 0
    while not isfile:
        isfile = os.path.isfile(json_file_name)
        if isfile:
            return json_file_name
        else:
            print('NO ESTA!!')
            time.sleep(5)
            counter = counter + 1
            if counter > 10:
                raise FileNotFoundError('Not scheduler json!!')

def create_ssh_file(json_file_name):
    #Test if file exists
    json_file_name = test_scheduler_file(json_file_name)
    #open file
    json_file = open(json_file_name)
    #load json in memory
    data = json.load(json_file)
    scheduler_addrs = data['address']
    scheduler_addrs = scheduler_addrs.replace('tcp://', '')
    scheduler_addrs = scheduler_addrs.split(':')[0]
    dashboard_port = data['services']['dashboard']
    dashboard_addrs = scheduler_addrs + ':' + str(dashboard_port)
    try:
        nodes = look_in_environment('SLURM_STEP_NODELIST')
    except KeyError:
        nodes = os.uname()[1]
    node_name = nodeset_like(nodes)
    log_name = look_in_environment('LOGNAME')
    tira_ssh = 'ssh -t -L {}:localhost:{} {}@ft3.cesga.es ssh -L {}:{} {}'.format(
        dashboard_port,
        dashboard_port,
        log_name,
        dashboard_port,
        dashboard_addrs,
        node_name[0]
    )
    print(tira_ssh)
    return tira_ssh

def create_dask_client(json_file_name):
    #Test if json scheduler exist
    from distributed import Client
    json_file_name = test_scheduler_file(json_file_name)
    print(json_file_name)
    #json_file_name = os.stat(json_file_name)
    dask_client = Client(scheduler_file=json_file_name)
    return dask_client

if __name__ == "__main__":
    FLAGS = None
    parser = argparse.ArgumentParser()
    parser.add_argument('-local', default='/tmp', help='Local Storage')
    parser.add_argument(
        "--scheduler",
        dest="scheduler",
        default=False,
        action="store_true",
        help="Launch only the scheduler",
    )
    parser.add_argument(
        "--worker",
        dest="worker",
        default=False,
        action="store_true",
        help="Launch only the worker(s)",
    )
    parser.add_argument(
        "--dask_cluster",
        dest="dask_cluster",
        default=False,
        action="store_true",
        help="Deployment of complete Cluster",
    )
    parser.add_argument(
        "--ssh_file",
        dest="ssh_file",
        default=False,
        action="store_true",
        help="Create file with ssh tunelling command",
    )
    parser.add_argument(
        "-scheduler_address",
        dest="scheduler_address",
        type=str,
        default=None,
        help="Scheduler IP. ex: tcp://10.120.10.7:8085",
    )
    parser.add_argument(
        "--client",
        dest="client",
        default=False,
        action="store_true",
        help="Get the dask client",
    )
    FLAGS, unparsed = parser.parse_known_args()

    if FLAGS.scheduler:
        scheduler_dask = launch_scheduler()
    if FLAGS.worker:
        if FLAGS.scheduler_address is not None:
            worker_dask = launch_worker(
                scheduler_address=FLAGS.scheduler_address,
                local_folder=FLAGS.local
            )
        else:
            #Verifica que el json de configuracion existe
            json_file = test_scheduler_file('./scheduler_info.json')
            worker_dask = launch_worker(
                scheduler_file='./scheduler_info.json',
                scheduler_address=FLAGS.scheduler_address,
                local_folder=FLAGS.local
            )

    if FLAGS.ssh_file:
        ssh_file = create_ssh_file('./scheduler_info.json')
        f = open("./ssh_command.txt", "w")
        f.write(ssh_file)
        f.close()

    if FLAGS.client:
        dask_client = create_dask_client('./scheduler_info.json')
        print(dask_client)

    if FLAGS.dask_cluster:
        try:
            os.remove("./scheduler_info.json")
        except FileNotFoundError:
            print('Ya esta borrada')

        final_id = int(look_in_environment('SLURM_PROCID'))

        if final_id == 0:
            scheduler_dask = launch_scheduler()
        else:
            #Todos las tareas worker comprueban que existe el fichero
            #de configuracion del cluster
            test_scheduler_file('./scheduler_info.json')
            if final_id == 1:
                #Solo una me crea el fichero de configuracion de tunelling
                ssh_file = create_ssh_file('./scheduler_info.json')
                f = open("./ssh_command.txt", "w")
                f.write(ssh_file)
                f.close()
            worker_dask = launch_worker(
                scheduler_file='./scheduler_info.json',
                scheduler_address=None,
                local_folder=FLAGS.local
            )
