import time


## Defino backoff exponencial
def backoff(n_retries=4):
    def _backoff(fn):
        def wrap(*args, **kwargs):
            for n in range(1, n_retries+1):
                print(f"Ejecutando funcion '{fn.__name__}' con backoff exponencial de {n_retries} intentos. Intento {n}:")
                try:
                    result = fn(*args, **kwargs)
                    print(f"'{fn.__name__}' ejecutada con exito y termina backoff")
                    break  ## Si la fn decorada NO arroja ninguna excepcion, sale del backoff!
                except Exception as e:
                    if n == n_retries:
                        print("Retries ejecutados sin exito, el programa se detiene acá.")
                        raise e
    
                    print(f"Hubo una excepcion tipo '{e}'. Se ejecutará el retrie no. {n}, el tiempo de espera es {2**n} segundos")
                    time.sleep(2**n)
            return result
        return wrap
    return _backoff
