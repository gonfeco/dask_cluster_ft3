from dask import delayed
from dask_jobqueue import SLURMCluster
from distributed import Client, wait
from time import time

from inc_dask_module import inc

from dask_cluster import create_dask_client

#Aqui intentamos conectarnos al cliente
info = "./scheduler_info.json"
client = create_dask_client(info)

data = [1, 2, 3, 4]
result = []

#Enviamos el calculo al cluster
for x in data:
    y = client.submit(inc, x)
    result.append(y)


inicio = time()
#recogemos el calculo del cluster

final = client.gather(result)
#final = result.result()
fin = time()

print("Resultados= {}".format(final))
print("Tiempo= {}".format(fin-inicio))

#Cerramos el cliente para que todo acabe bien
client.shutdown()
