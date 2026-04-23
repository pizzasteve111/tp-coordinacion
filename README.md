# Informe

Se realizaron distintas modificaciones sobre el esqueleto base provisto. 

## Gateway

La principal modificación viene en su message Handler, donde se genera un clientId aleatorio e interno(no se comparte con client) para mantener seguimiento de sus mensajes y poder escalarlo a cuando hayan varios clientes.

Se mantiene un registro de los mensajes/frutas totales que comparte el cliente, la idea es que una vez tenemos su EoF, poder avisar a las demas entidades cuantos son los mensajes finales que corresponden a cada cliente.

## Sum

Se implementa un sistema de persistencia en disco para evitar cuellos de botella por  falta de memoria. La idea es que cada instancia de sum o aggregator tengan su propio storage para cada cliente, cuando una instancia Sum recibe el Eof de gateway, lo que hace es broadcastear por un middleware hacia todos los otros sums, indicando la totalidad de mensajes que envió ese cliente. Este canal se consume en una go routine a parte al input/output que ya se tiene, el procesamiento es accedido mediante un lock.

Todos los sums al recibir el eof lo que hacen es compartir cuantas tasks procesaron de ese cliente, si ven que en la totalidad de la distribución ya tienen el total de mensajes de un client, cada una genera suma y la envía a aggregator.  Ejemplo: Sum1 publica que tiene 35 de las 100 tareas de client X, sum2 que hizo otras 35 y sum 3 que hizo otras 30, al enterarse ven que ya se llegó a cien y mandan sus resultados


## Aggregator

Se hace que el enrutamiento de mensajes desde sum se haga mediante discreción por frutas. Es decir, la fruta X siempre es procesada por Agg i que es calculado determinísticamente con una función de hashing. Cada aggregator realiza un top K parcial de las frutas con las que trabaja y luego se lo envía a Join.


La solución global va a ser optima por que cada aggregator muestra la mayor cantidad de ocurrencias entre un único e irrepetible grupo de frutas. Por ejemplo, entre manzana, banana,mango y pera. Si devuelve un top que no incluye a pera.


## Join

Espera a recibir los mensajes de todos los aggregators para con un client. Además mantiene un seguimiento de los total tasks por client. Una vez le llegan todos los eof/resultados de aggregator, ahí genera un top global en base a los parciales. El cual es óptimo y determinístico ya que cada top parcial es en base a un grupo único de frutas

## Storage

La lógica de storage busca que no se mantenga en memoria muchos datos, a la hora de procesar un archivo, se busca que se itere y consuma a la vez. Me terminó quedando un poco de código repetido en las tres implementaciones, como en clase se dijo que al final no era necesario ocuparse de esto, no le terminé dando el tiempo necesario para refactorizarlo

# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.
