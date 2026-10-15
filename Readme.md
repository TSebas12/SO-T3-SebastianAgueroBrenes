# Cómo trabaja `box_meeseeks.c`

Compilar: `gcc -Wall -Wextra -O2 -o box_meeseeks box_meeseeks.c -pthread -lm`
Usar: `./box_meeseeks [timeout_caos_segundos]`

## 1. Idea general

1. El **Box** lee una solicitud por teclado en `box_loop()`.
2. Detecta si es **A-textual, B-cálculo (`calc:`), C-externo (`exec:` / `run:`)**.
3. Si es A, pide **dificultad 0-100** y **duración 0.5-5 s**.
4. Crea dos `pipe()`: uno para mandar la solicitud y otro para recibir el resultado.
5. Hace `fork()` y nace el **Meeseeks inicial N=1, i=1** que ejecuta `rutina_meeseeks()`.
6. El Box espera con `waitpid()`, lee el resultado del pipe, mide el tiempo y guarda el reporte.
7. Repite hasta leer `adios / bye / exit / quit`, ahí imprime el informe final.

```
[teclado] → BOX --pipe solicitud--> Meeseeks N=1,i=1
              ^                          |
              +--pipe resultado-----------+--fork--> hijos N+1, N+2...
              +--waitpid + reporte + informe final
```

Todo mensaje viaja por `pipe()`. Solo los contadores (`total_meeseeks`, `tarea_actual`, `nivel_max`) están en memoria compartida.

## 2. Estructuras y constantes

```c
#define MAX_TEXTO 512
#define MAX_TAREAS 100
#define MAX_NIVEL 6
#define MAX_MESEEKS_TAREA 60
```

- `ContadoresShared`: `mutex` + `total_meeseeks` + `nivel_max` + `tarea_actual`. Vive en `mmap(MAP_SHARED)` y se protege con `sem_wait / sem_post`.
- `ResultadoPipe`: `exito` + `solver_pid, solver_ppid, solver_N, solver_i`. Es lo que el hijo le escribe al padre por el pipe.
- `Reporte`: `solicitud, tipo, dificultad, dur_sim, t_total, t_real, exito, num_meeseeks, solver_*`. Solo lo usa el Box.

## 3. `main()`

1. Quita el buffer de `stdout` para no duplicar mensajes en el `fork()`.
2. Lee `argv[1]` como timeout del caos (`0` = sin límite).
3. Crea la memoria compartida con `mmap()` e inicializa el semáforo con `sem_init(...,1,1)`.
4. Si hay timeout, arma `alarm()` con `sigaction()` hacia `manejador_caos()`.
5. Entra a `box_loop()`. Al salir limpia la alarma, destruye el semáforo y libera el `mmap()`.

## 4. `box_loop()` — lo que hace el Box por cada pedido

```
lee línea -> trim_nl -> detectar_tipo()
si es A: pedir_dificultad() + pedir_duracion()
pipe(sol) + pipe(res)
t0 = ahora_seg()
fork()
  hijo: lee solicitud del pipe_sol, llama rutina_meeseeks(...,1,1), _exit()
  padre: escribe solicitud en pipe_sol, waitpid(hijo), lee pipe_res, t1 = ahora_seg()
t_total = t1 - t0
t_real = t_total - (dur_sim si es A, si no 0)
guarda Reporte e imprime [OK] o [FALLO]
```

`detectar_tipo()`: si empieza con `exec:`/`run:` es C, si empieza con `calc:` es B, si solo tiene números y operadores (`+-*/%(). <>=!&|`) también es B, lo demás es A.

## 5. `rutina_meeseeks()` — lo que hace cada Meeseeks

Todos los Meeseeks corren la misma función, solo cambian `N` (nivel) e `i` (instancia en ese nivel):

1. Guarda su `pid, ppid, N, i` e instala el handler de `SIGTERM`.
2. Suma 1 a los contadores compartidos.
3. Saluda: `Hi, I'm Mr. Meeseeks! Look at Meeeee.`
4. Hace su trabajo según el tipo:
   - **B:** limpia el `calc:`, evalúa con `evaluar_expresion()`, no crea hijos.
   - **C:** limpia el `exec:`, corre `ejecutar_externo()`, no crea hijos.
   - **A:** duerme `dur_sim` con `nanosleep()`, tira el dado con `casoA_intentar(d)`. Si falla y `d < 85.01`, calcula `d_hija` con `aging_dificultad()` y llama a `crear_hijos()`. Si `d >= 85.01`, falla sin hijos.
5. Se despide con `POOF!`, escribe su `ResultadoPipe` en `fd_res` y retorna `1` éxito / `0` fallo.

## 6. `crear_hijos()` — cómo se ramifica el caso A

- Decide cantidad: `d_hija <= 45` → **5 hijos**, si no → **2 hijos**.
- Reserva lugar con `reservar_meeseeks()` para no pasar de 60. Si `N+1 > 6`, no ramifica.
- Por cada hijo crea `pipe_sol + pipe_res` privados y hace `fork()`:
  - el hijo lee la solicitud de su pipe y corre `rutina_meeseeks(..., N+1, k+1)`.
  - el padre le escribe la solicitud y se queda con el extremo de lectura del resultado en modo no bloqueante.
- Espera en un ciclo con `nanosleep(50ms)`:
  - lee cada pipe de resultado. El primero que reporte `exito=1` gana.
  - al haber ganador, manda `kill(hermanos, SIGTERM)` para que se despidan.
  - a los que ya murieron los recoge con `waitpid(WNOHANG)`.
- Devuelve `1` si algún subárbol ganó y copia quién fue el héroe en `res_out`, que luego se propaga hacia arriba.

## 7. Caso A: dado y aging

`casoA_intentar(d)`:

```c
si d <= 0 return 0;      // imposible, nunca gana
si d >= 100 return 1;    // trivial, siempre gana
tiro = rand(0..100);
return tiro <= d;
```

`aging_dificultad(d_padre, num_hijos, nivel)`:

```c
si d_padre == 0 return 0;  // lo imposible no se diluye
d = d_padre + (100 - d_padre)/num_hijos + 2*nivel;
si d > 99 d = 99;
```

Cada nivel hijo es más fácil, por eso converge, menos cuando `d=0` que se queda en `0` y agota los topes.

## 8. Caso B: calculadora

Parser recursivo con esta precedencia:

```
|| → && → == != < > <= >= → + - → * / % → unario (- ! +) → número o (expr)
```

- `* / %` usan chequeo de división por cero y `fmod()` para el módulo.
- `evaluar_expresion()` devuelve `1` y el valor, o `0` y el texto del error.

## 9. Caso C: programa externo

1. Parte la línea por espacios con `strtok()` en `argv[]`.
2. `fork()` + `execvp(argv[0], argv)` + `waitpid()`.
3. Éxito solo si termina normal y con `rc == 0`.

## 10. Comunicación entre procesos

- **Pipes:** solicitud Box→hijo y resultado hijo→padre. Cada grupo de hijos tiene sus propios pipes privados.
- **Memoria compartida + semáforo:** solo contadores. Todo acceso va entre `sem_wait()` y `sem_post()`.
- **Señales:** `SIGTERM` a hermanos cuando uno resuelve, cada Meeseeks se despide y hace `_exit(2)`. El caos hace `kill(0, SIGTERM)`, luego `SIGKILL` y `_exit(99)`.

## 11. Tiempos

- `ahora_seg()` usa `CLOCK_MONOTONIC`.
- `t_total = t1 - t0` (de `fork()` a `wait()`).
- `t_real = t_total - dur_sim` solo en A. En B y C `t_total == t_real` porque no hay sleep de simulación.

## 12. Ejemplo de cada flujo

| Entrada | Qué hace el código |
|---|---|
| Texto libre, `d=95` | 1 Meeseeks duerme `dur_sim`, tira dado con 95% de ganar, responde por pipe, `[OK]` |
| Texto libre, `d=60` | falla, crea 2 hijos N=2 con `d_hija = 60+40/2+2 = 82`, reintentan, el ganador sube por pipes, `[OK]` con x3 |
| Texto libre, `d=0` | siempre falla, ramifica 5 en 5 hasta llenar 60 y nivel 6, todo `[FALLO]` |
| `calc: 3+4*2` | 1 Meeseeks evalúa `*` antes que `+` → `11`, `[OK]` |
| `calc: 5/0` | detecta divisor 0 → error, `[FALLO]` |
| `exec: echo hola` | `fork+execvp+wait`, `rc=0` → `[OK]` |
| `exec: false` | `rc=1` → `[FALLO]` |
