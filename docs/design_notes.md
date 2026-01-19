## Modelado
Con base en el json de muestra y las notas dadas, en las que se menciona que "items" es un array, se entiende que cada orden puede contener varios items; por lo cual, siguiendo a Kimball, la fact table se diseña con el grano mas fino, que en este caso es la informacion que viene en items (cantidad y precio). NO definí la fact table como "fact_order" como se sugería en el enunciado; en su lugar, almaceno los atributos de la orden (amount, currency, source, promo) en una dimension independiente (dim_order), y defino la fact table como "fact_sales", considerando que una venta neta es la cantidad de productos por su respectivo precio unitario. De esa manera puedo responder fácilmente preguntas relativas a productos, como por ejemplo "productos más vendidos". 

En `sql/duckdb_ddl.sql` está el esquema de las 4 tablas: dim_order, dim_product, dim_user, fact_sales.


#### Queries de ejemplo (se ejecutaron con duckdb)

*Top 5 de ventas por usuario*
```
select user_id, sum(unit_price*quantity) total_vendido
from fact_sales
group by 1
order by 2 desc
limit 5
```

*Top 5 de productos más vendidos*
```
select name, sum(quantity) cantidad_unidades
from fact_sales fs
join dim_product du
on fs.sku = du.sku
group by 1
order by 2 desc
limit 5
```

*Top 5 de paises con mas ventas*
```
select country, sum(unit_price*quantity) total_vendido
from fact_sales fs
join dim_user du
on fs.user_id = du.user_id
group by 1
order by 2 desc
limit 5
```

*Canal por el que se hicieron más ventas*
```
select dor.source, sum(unit_price*quantity) total_vendido
from fact_sales fs
join dim_order dor
on fs.order_id = dor.order_id
group by 1
order by 2 desc
```


## Data de input
Le pedí a un LLM que generara el archivo json con "realistic fake data" que simula la respuesta del API, con base en el ejemplo dado y las condiciones del enunciado, a saber:
- Fechas en formato ISO 
- Registros con order_id duplicado
- Uno o más item por orden
- Registros con created_at null o sin items o sin metadata

Las tablas "users" y "products" tambien las generó sinteticamente un LLM en formato csv, y son consistentes con los ids del json generado.

El output dado por el LLM es el que está en `sample_data`.


## Transformaciones: Polars en lugar de PySpark o Pandas
Elegí trabajar con polars en lugar de PySpark por practicidad para encarar una de los situaciones planteadas, y es que la fuente principal de datos es semi-estructura (json), y en ese caso, el metodo `json_normalize()` resulta **muy util** para estructurar esa data en forma tabular, que es finalmente como será almacenada en el Data Warehouse. PySpark no cuenta con ese metodo `json_normalize()`, ni siquiera usando la api de pandas de PySpark, porque pandas pone problema por los tipos de datos, a diferencia de polars cuyos tipos de datos son homologables a los de db relacionales. Entonces esa es la razón principal por la cual elegí trabajar con polars para hacer las transformaciones. Además, polars es más eficiente que pandas, y puede ser igual de eficiente que Spark porque tambien permite aprovechar todos los nucleos de cpu para distribuir el procesamiento, y tambien permite lazy evaluation (no usado en este caso).

Respecto a la **robustez** mencionada en el enunciado, así los datos vengan malformados del api, el metodo `json_normalize()` de polars los gestiona con null. De modo que al aplicarlo, se mapean todos los campos de todos los niveles del json (controlable con parametro "max_level") y deja null donde corresponda, es decir, si un registro no tiene un campo que otro registro sí. Así, a pesar de que no todos los registros del json compartan los mismos campos, se obtiene un DataFrame con la data integra, y con el que se pueden realizar operaciones como explode() y unnest() para conseguir los objetivos planteados en el modelado.


## Retries
La estrategia usada para gestionar retries es aplicar un backoff exponencial. Ese backoff lo definí como un decorador en `src/retries_config.py`. Ese decorador lo apliqué sólo a la funcion `get_api_data()`, encargada de simular los "llamados al api", pues esa funcion es la susceptible de levantar excepciones por respuestas tipo 5xx, por ejemplo, que son caidas temporales, y en esos casos basta con esperar algunos segundos e intentar nuevamente el request, evitando que el job se caiga y tener que ejecutar manual todo nuevamente. Definí el decorador para que tome como parametro la cantidad de reintentos permitidos. Por defecto son 4.


## Idempotencia
Para lograr este objetivo, tomé como referencia el funcionamiento de terraform, el cual crea y almacena en un archivo .tfstate la configuración actual de la infra levantada, de modo que cuando se añade un nuevo recurso a la infra, no levanta todo nuevamente sino consulta el estado actual en el .tfstate y lo compara con el estado deseado. Así, solo levanta lo que no coincida con el estado actual. Con esa idea en mente, definí un archivo `state.json` que almacena el "order_id" y el"created_at" de la orden con la fecha mayor, es decir, la fecha más reciente de todo el conjunto de registros procesado. Luego, cada vez que se ejecuta el job, se llama a la funcion `get_current_state()` que obtiene la fecha almacenada en `state.json` y evalua que en los registros "del api" no exista un registro con el mismo "created_at" y "order_id" del state.json; de NO ser así se termina el programa y no se ejecuta el resto del job. Ojo! en un caso real no se cosumiria todo el api, la condicion sería que devuelva solo registros con "created_at" > current_state.

Para gestionar los casos borde en que varios registros compartan el mismo "created_at", y que ese "created_at" quede en state.json representando el "estado actual" se usa el "order_id" (suponiendo ese id como un incremental). Esto se comenta en las lineas 61 a 71 de transforms.py.


## Monitoreo
En producción (nube) lo que haría es: empaquetar mi codigo fuente en una imagen de docker, mandarla a un artifact registry, y desplegarla en un servicio serverless como Cloud Run (GCP) o Lambda (AWS). Cualquiera de esos servicios se integran con la suite de monitoring (CloudWatch, CloudMonitoring) de la respectiva nube, donde se pueden consultar las principales métricas de interes: cantidad de ejecuciones, recursos utilizados (cpu, memoria), y la trazabilidad que dejan los logs. Lo importante con respecto a ese tema de los logs es que el programador incluya "puntos de control" (prints) en su código, de acuerdo al flujo de ejecución, para en caso de tener que debuggear, encontrar facilmente la fuente del problema. 

En nuestro caso, en el codigo incluí una serie de prints que permiten llevar trazabilidad de cada ejecución. Suponiendo que el ETL se ejecute on-prem (donde no se cuenta con servicios de monitoreo integrado como en la nube), se definió la funcion `setup_loggin()` que permite redirigir los prints a un archivo logs.log para poder almacenar el historico de ejecuciones. En ese caso descomentar la linea 2 de `etl_job.py`.

Además, en el script principal se gestionan las excepciones invocando la funcion `send_mail()` que notifica por correo la alerta en caso de que una ejecución falle, de modo que se pueda gestionar rapidamente el eventual bug.


## Carga a Redshift
Probablemente armaría un DAG de Airflow con dos task: uno que ejecute el job (la imagen de docker segun lo expuesto en la seccion anterior), y otro dependiente del primero que ejecute los COPY para cargar de S3 a Redshift.