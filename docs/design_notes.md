## Modelado
Con base en el json de muestra y las notas dadas, en las que se menciona que "items" es un array, se entiende que cada orden puede contener varios items; por lo cual, siguiendo a Kimball, la fact table se diseña con el grano mas fino, que en este caso es la informacion que viene en items (cantidad y precio). NO definí la fact table como "fact_order" como se sugería en el enunciado, en su lgugar almaceno los atributos de la orden (amount, currency, source, promo) en una dimension independiente (dim_order), y definí la fact table como "fact_sales", considerando que una venta neta es la cantidad de items de la orden por su respectivo precio unitario. De esa manera puedo responder fácilmente preguntas relativas a la cantidad de productos, como por ejemplo "productos más vendidos". 

En `sql/..ddl.sql` está el esquema de las 4 tablas: dim_order, dim_product, dim-user, fact_sales


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
- Uno o más sku por orden
- Registros con created_at null o sin items o sin metadata

Las tablas "users" y "products" tambien los generó sinteticamente un LLM en formato csv, consistente con los ids de generados en el json

El output dado por el LLM es el que está en `sample_data`



## Polars en lugar de PySpark o Pandas
mencionar acá que los campos los json malformados (campos faltantes) polars los gestiona con null
**robustez**



## Retries
La estrategia usada para gestionar retries es aplicar un backoff exponencial. Ese backoff lo definí como un decorador en `src/retries_config.py`. Ese decorador lo apliqué sólo a la funcion `get_api_data()`, encargada de simular los "llamados al api", pues esa funcion es la susceptible de levantar excepciones por respuestas tipo 5xx, por ejemplo, que son caidas temporales, y en esos casos basta con esperar algunos segundos e intentar nuevamente el request, evitando que el job se caiga y se tener que ejecutar manual todo nuevamente. Definí el decorador para que tome como parametro la cantidad de reintentos permitidos. Por defecto son 4.


## Idempotencia
Para lograr este objetivo, tomé como referencia el funcionamiento de terraform, el cual crea y almacena en un archivo .tfstate la configuración actual de la infra levantada, de modo que cuando se añade un nuevo recurso a la infra, no levanta todo nuevamente sino consulta el estado actual en el .tfstate y lo compara con el estado deseado. Así, solo levanta lo que no coincida con el estado actual. Con esa idea en mente, definí un archivo `state.json` que almacena el "order_id" y



## Monitoreo
El tema de log logs depende del flujo de ejecución del codigo... se
