Client no se puede modificar
Gateway solo el message handler.

En las inputs se mandan msjs, de las outputs se leen.

Distribuimos Sum, tenemos varias instancias que son balanceadas por las working queue input que encola el gateway.
De esa forma ya podes pasarle distintos fragmentos de cantidad a cada instancia.
Sin embargo, solo una recibe el EoF, esto quizas haciendo un mecanismo de concurrencia con una barrier o condition.

Cuando una sola recibe el EoF, se lo triggerea y se ponen todas las instancias en trabajo terminado.
Otra sería tener una exchange queue que le llegue a todas las suscritas el mensaje de EoF y ahí terminar de procesar.

Cada sum manda por su queue sus sumas de X frutas, luego estas tambien se distribuyen por la working queue hacia las instancias aggregator que hace el top
cada una, esto lo mandan a la queue de Join. Aggregator puede recibir suma de cualquiera de las instancias de Sum.



Join va a recibir los tops de los distintos aggregations y va a calcular su propio top y le responde al gateway.
Por cuestiones de evitar multiples ejecuciones, lo ideal sería que el join de tops se haga una vez recibieron todos los mensajes de los aggregators.
Entonces tiene que haber una forma de sincronización entre los N aggregators para que una vez que todos terminaron de comunicar a Join, se mande el mensaje EoF a Join

Cuando Join lee este mensaje, sabe que tiene que procesar el top final y comuinicarlo a client.

Si Join conoce la cantidad de aggregators, lo hace una vez


Idea: Si puedo modificar el message handler, hacer que en cada serialización se indique el id y que en el EoF se indique la cantidad total de mensajes por cada client.


Gateway no se puede modificar, invoca como queues solo working, asi que entiendo que como tal no es capaz de broadcastear.

Se puede modificar el message Handler que usa gateway, hacemos que este mantena un seguimiento de las N porciones en las que se divide los mensajes del cliente.

Cuando recibe EoF, manda por la working queue que hay N mensajes en total.

El sum que reciba el EoF broadcastea a todas las workings que ya se terminó el flujo de mensajes desde gateway, que ya no deberían esperar mas mensajes.
Entonces cada Sum ahora ya puede procesar todos sus datos y enviar el resultado a Agg, tanto sum como agg van guardando sus datos en un archivo para no tenerlo cargado en memoria.
Cuando reciben el eof ahí lo itean(yield) y lo procesan.

El mensaje de cada sum es:
payload, X tareas completadas de N totales

Entonces un aggregator sabe cuantas tareas ya procesó de las N que le pueden llegar.


Los aggregators reciben mensajes de Sum, los persisten por client id, si esta persistencia alcanza un tamaño N
o pasa un tiempo T, lo procesa generando un resultado parcial y limpia ese archivo, le pasa a Join el resultado parcial y le dice que eso le corresponde a X de las N tasks originales de gateway. Cuando join recibe todos los resultados parciales y ve que eso ya completa todas las tasks de un client, les dice a todos los aggregators que pueden borrar el archivo de clientX, esto es solo la conveniencia de borrar un archivo vacío.
Terminan de esperar mensajes y mandan sus resultados a Join indicando "procese X tareas de las 150" en total.

Join va a mantener un seguimiento de todas las tareas que procesen los Aggs hasta que se llegue al máximo.

# Tests:

Para uno de cada uno se reciben los resultados esperados.

cd /mnt/c/Users/juanc/tp-coordinacion/golang

## Test 2:

Cada client tiene un message handler con el que traduce la info de sus mensajes al protocolo interno del sistema.

Falla porque al no ser distribuido, todos los mensajes de los clientes se los juntan
y se devuelve el resultado como si fuera uno solo.

Solución: En el message handler, ademas de caso eof y expresar cuantas tareas de cada uno, indicar de quien es el EoF y las tareas de cada client.

Las instancias sum o aggregator tienen que discernir el procesamiento para cada uno de los clients.

En este caso me piden pasar todos los tests con una replica de cada elemento.

Lo que priorizamos entonces es la persistencia en disco y dividir el procesamiento de acuerdo al client.

Tanto el mensaje como EoF de message handler tiene que tener el id de client.


 ahora sum tiene que mantener un seguimiento de tasks por cada client.
 Para no tener cargado en memoria muchos mensajes, lo ideal es que cree un json donde persiste los fruit records para cada client. Hay un directorio de storage para cada sum y un archivo de cada client de cada sum, así evitamos leer información extra a la hora de tener que escribir un archivo.
 cada sum tiene su SumStorage.json donde para cada client tiene sus fruits.
Cuando un storage de client pasa de 10mb o se cumple un timeout, ese sum procesa el archivo y manda el resultado
flusheando el archivo y dejandolo limpio. Sum entonces podría hacer varios procesamientos por cada client, pero nos ahorramos tener que sincronizar todas las instancias. Si queremos evitar tanto procesamiento, basta con aumentar el tamaño permitido o timeout.
Si recibe EoF del gateway, simplemente se lo manda a Aggregator para que le llegue a join.
Cuando genera un resultado comunica al aggregator cuantas frutas proceso de ese cliente.
Así Join va a conocer las frutas totales por cliente y, a medida que le lleguen los resultados de aggregator,
va a saber si ya termino de recibir frutas para un cliente en particular.

Con este enfoque, evitamos tener muchos datos en memoria, persistimos todo lo que podemos en disco, pero evitamos tener que hacer lecturas muy demandantes a la hora de escribir.
.