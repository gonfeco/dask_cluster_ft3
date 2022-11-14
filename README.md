Librería que permite levantar cluster de dask de forma sencilla en el FT-III

Ficheros de la librería:

* dask\_cluster.py: script python con todas las funciones necesarias para levantar el *scheduler* y los *workers* de un *cluster* de **dask**. Se puede utilizar como una línea de comandos:
    * python  dask\_cluster.py -h: nos da una lista con los posibles inputs:
    * python  dask\_cluster.py  --scheduler: levanta solo un *scheduler*
    * python  dask\_cluster.py --worker: levanta un  worker y lo conecta al cluster especificado en el fichero **scheduler\_info.json** (el fichero **debe existir**)
    * python  dask\_cluster.py --worker -scheduler\_address tcp://10.120.10.7:8085 : levanta 1 worker y lo conecta al *scheduler* ubicado en la dirección IP proporcionada (en el ejemplo lo conectaría a un scheduler en tcp://10.120.10.7:8085)
    * python  dask\_cluster.py --ssh\_file: permite crear el fichero con el comando necesario para la redirección de puertos para conectarse al dashboard de dask (el fichero creado siempre será el mismo: **ssh\_command.txt** y se creará en el directorio donde se lance el *scheduler*)
* dask\_submit\_workers.sh: script en bash que lanza al sistema de colas varios *workers*. Lanza tantos workers como se le indique en el número de tareas de la reserva!! El script está diseñado para utilizar un fichero: **scheduler\_info.json** que genera el *scheduler*. Para hacer esto debe primero existir un *scheduler* levantado!!
* dask\_cluster\_submit.sh: script que lanza a cola el cluster completo (*scheduler* y *workers*). La reserva de recursos debe ser tal que el número de tareas solicitadas sea igual al número de *workers* deseados + 1 (que será la tarea que se encargue del *scheduler*).
