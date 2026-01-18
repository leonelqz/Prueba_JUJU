# Prueba_JUJU

- Instala depedencias:
```
pip install -r requirements.txt
```

- Corre el job con todos los registros disponibles en el json que simula el api:
```
python -m src.etl_job
```

- Usa el flag `--last-processed` (no recibe argumentos, es booleano) para consultar el último registro procesado, usado como referencia en el código para evitar volver a procesar (idempotencia):
```
python -m src.etl_job --last-processed
```

- Corre el job a partir de una fecha dada (flag `--since`): 
```
rm state.json
python -m src.etl_job --since 2025-12-24T00:00:00
```
**Nota:** como en sample_data en ambos casos (con y sin `--since`), el registro con la fecha más reciente es el mismo, para que se ejcute el job con el flag `--since`, elimina state.json, pues la idempotencia se evalua consultando en ese archivo el "estado actual" con base en el registro procesado con la fecha más reciente en la anterior ejecución.

 Para ver la diferencia del resultado con y sin `--since`, ir a `output/raw/` y consultar el json generado por cada ejecucion (click derecho > format document). En el primer caso contiene  3626 lineas; en el segundo  24474 lineas.
