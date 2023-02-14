"""
Script que permite levantar cluster de dask en el FT3
"""
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

    Parameters
    ----------
    environment_variable: Variable de entorno de la que se quiere
        saber el valor. Si no la hay levanta un error.

    Returns
    ----------

    value_variable: Valor de la variable de entorno si se consigue leer
        correctamente.

    """
    try:
        value_variable = os.environ[environment_variable]
        #print flag
    except KeyError:
        print("Not "+environment_variable+" environment variable!!")
        raise KeyError
    return value_variable

def launch_scheduler(scheduler_file=None, preload=None):
    """
    Lanza el scheduler del cluster

    Parameters
    ----------

    scheduler_file : path con el nombre donde el scheduler guardara su
        informacion
    preload : path del script python que se le pasa al scheduler para
        que tenga todos los modulos necesarios para su ejecución

    Returns
    ----------

    process : instancia al scheduler
    """

    if scheduler_file is None:
        print("BE AWARE!!. Not provide scheduler_file: ./scheduler_info.json"\
            " will be used for store scheduler info!!")
        scheduler_file = "./scheduler_info.json"

    try:
        ports = look_in_environment('SLURM_STEP_RESV_PORTS')
        port_list = nodeset_like2(ports)

    except KeyError:
        ports = None
        port_list = None
    local_id = int(look_in_environment("SLURM_LOCALID"))

    dask_scheduler = "dask-scheduler --dashboard --interface ib0" \
        " --scheduler-file {}".format(scheduler_file)
    #dask_scheduler = "dask-scheduler --dashboard --dashboard-address 36015 --interface ib0" \
    #    " --scheduler-file {}".format(scheduler_file)
    if port_list is not None:
        print(port_list[local_id])
        dask_scheduler = dask_scheduler + " --port {}".format(port_list[local_id])
    if preload is not None:
        dask_scheduler = dask_scheduler + " --preload {}".format(preload)
    print('Command line to create Scheduler: {}'.format(dask_scheduler))
    #Lanzamos el comado que monta el Scheduler
    process = subprocess.run(dask_scheduler.split(), stdout=subprocess.PIPE)
    return process

def launch_worker(
        scheduler_file=None,
        scheduler_address=None,
        local_folder='/tmp',
        preload=None
    ):
    """
    Lanza un worker

    Parameters
    ----------

    scheduler_file : path al json con la info del scheduler.
    scheduler_address : tcp addres del scheduler. Se usa sino se le
        proporciona un scheduler_file.
    preload : path del script python que se le pasa al worker para
        que tenga todos los modulos necesarios para su ejecución

    """
    try:
        ports = look_in_environment('SLURM_STEP_RESV_PORTS')
        port_list = nodeset_like2(ports)

    except KeyError:
        ports = None
        port_list = None
    local_id = int(look_in_environment("SLURM_LOCALID"))
    worker = "dask-worker  --interface ib0 --no-nanny --nthreads 1" \
        " --local-directory {}".format(local_folder)

    if port_list is not None:
        print(port_list[local_id])
        worker = worker + " --worker-port {}".format(port_list[local_id])
    print("scheduler_file: {}".format(scheduler_file))
    print("scheduler_address: {}".format(scheduler_address))
    if scheduler_file is not None:
        #Primero testeo que el fichero scheduler existe
        json_file = test_scheduler_file(scheduler_file)
        worker = worker + " --scheduler-file {}".format(scheduler_file)
    elif scheduler_address is not None:
        worker = worker.replace(
            "dask-worker",
            "dask-worker {}".format(scheduler_address)
        )
    else:
        raise ValueError('No tengo direccion del scheduler')
    if preload is not None:
        worker = worker + " --preload {}".format(preload)
    print('Command line to create worker: {}'.format(worker))
    #Lanzamos el comando que levanta el Worker
    process = subprocess.run(worker.split(), stdout=subprocess.PIPE)
    return process

def test_scheduler_file(json_file_name="./scheduler_info.json"):
    time.sleep(5)
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
        node_name = nodeset_like(nodes)[0]
    except KeyError:
        node_name = os.uname()[1]
    log_name = look_in_environment('LOGNAME')
    tira_ssh = 'ssh -t -L {}:localhost:{} {}@ft3.cesga.es ssh -L {}:{} {}'.format(
        dashboard_port,
        dashboard_port,
        log_name,
        dashboard_port,
        dashboard_addrs,
        scheduler_addrs
    )
    print(tira_ssh)
    f_pointer = open("./ssh_command.txt", "w")
    f_pointer.write(tira_ssh)
    f_pointer.close()
    return tira_ssh

def create_dask_client(json_file_name="./scheduler_info.json"):
    #Test if json scheduler exist
    from distributed import Client
    json_file_name = test_scheduler_file(json_file_name)
    print(json_file_name)
    #json_file_name = os.stat(json_file_name)
    dask_client = Client(scheduler_file=json_file_name)
    return dask_client

if __name__ == "__main__":

    import argparse
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
        "-scheduler_file",
        dest="scheduler_file",
        type=str,
        default=None,#"./scheduler_info.json",
        help="File name for json file with the scheduler info",
    )
    #parser.add_argument(
    #    "--client",
    #    dest="client",
    #    default=False,
    #    action="store_true",
    #    help="Get the dask client",
    #)
    parser.add_argument(
        '-preload',
        dest="preload",
        default=None,
        help="File for preload code in dask scheduler and workers."
    )
    FLAGS, unparsed = parser.parse_known_args()

    if FLAGS.scheduler:
        scheduler_dask = launch_scheduler(
            scheduler_file=FLAGS.scheduler_file,
            preload=FLAGS.preload
        )
    if FLAGS.worker:
        worker_dask = launch_worker(
            scheduler_file=FLAGS.scheduler_file,
            scheduler_address=FLAGS.scheduler_address,
            local_folder=FLAGS.local,
            preload=FLAGS.preload
        )


    if FLAGS.ssh_file:
        ssh_file = create_ssh_file(FLAGS.scheduler_file)

    #if FLAGS.client:
    #    dask_client = create_dask_client(FLAGS.scheduler_file)
    #    print(dask_client)

    if FLAGS.dask_cluster:
        #try:
        #    os.remove("./scheduler_info.json")
        #except FileNotFoundError:
        #    print('Ya esta borrada')

        final_id = int(look_in_environment('SLURM_PROCID'))

        if final_id == 0:
            scheduler_dask = launch_scheduler(
                scheduler_file=FLAGS.scheduler_file,
                preload=FLAGS.preload
            )
        else:
            if FLAGS.scheduler_file is None:
                print("BE AWARE!!. Not provide scheduler_file:"\
                    "./scheduler_info.json"\
                    " will be used for reading scheduler info!!")
                scheduler_file = "./scheduler_info.json"
            else:
                scheduler_file = FLAGS.scheduler_file
            print("SOY EL WORKER: {}".format(scheduler_file))
            if final_id == 1:
                #Solo una me crea el fichero de configuracion de tunelling
                ssh_file = create_ssh_file(scheduler_file)
            worker_dask = launch_worker(
                scheduler_file=scheduler_file,
                local_folder=FLAGS.local,
                preload=FLAGS.preload
            )
