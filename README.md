# Prueba_JUJU

```
pip install -r requirements.txt
```

Correr el job con todos los registros disponibles en el "api":
```
python -m src.etl_job
```

Usa el flag `--last-processed` (no recibe argumentos, es booleano) para obtener el id y la fecha del último registro procesado, usado como referencia para evitar volver a procesar registros 
```
python -m src.etl_job --last-processed
```

Correr el job a partir de una fecha dada (flag `--since`): 
Como en ambos casos: con flag y sin flag, el registro procesado con la fecha más reciente es el mismo, para probar el flag `--since`, se debe eliminar state.json, pues la idempotencia se evalua consultando en ese arhcivo el registro con la fecha más reciente
```
rm state.json
python -m src.etl_job --since 2025-12-24T00:00:00
```

*Nota:* Para ver la diferencia del resultado sin y con `--since`, ir a `output/raw/` y consultar el json generado despues de cada ejecucion (click derecho > format document). En el primer caso contiene 24474 lineas; en el segundo 3626 lineas 
