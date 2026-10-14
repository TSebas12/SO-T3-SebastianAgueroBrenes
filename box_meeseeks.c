/* ============================================================================
 * Archivo      : box_meeseeks.c
 * Curso        : Sistemas Operativos - Tarea #3 Mr. Meeseeks
 * Autor        : [SuNombreCompleto] - [Carnet]
 * Lenguaje     : C sobre Linux (gcc -pthread -lm)
 * Compilar     : gcc -Wall -Wextra -O2 -o box_meeseeks box_meeseeks.c -pthread -lm
 * Uso          : ./box_meeseeks [timeout_caos_segundos]
 *                Default timeout = 300 s (5 min, base minima exigida).
 *
 * Descripcion general:
 *   Programa Box de Mr. Meeseeks. Recibe solicitudes de texto del usuario,
 *   crea (fork) un Mr. Meeseeks inicial (N=1,i=1) por solicitud y espera el
 *   resultado. Soporta 3 casos:
 *     A. Consulta textual  ("Quiero un cafe frances") -> simulacion con
 *        dificultad 0-100 y duracion 0.5-5 s, expansion jerarquica.
 *     B. Calculo aritmetico/logico ("calc: 3+4*2") -> un solo Meeseeks.
 *     C. Programa externo ("exec: ls -l /tmp") -> fork+exec+wait, exito=rc 0.
 *
 * Organizacion del codigo (diagrama):
 *
 *        +---------------- BOX (main/box_loop) -----------------+
 *        | lee stdin, detecta tipo A/B/C, pide dificultad/dur.  |
 *        | crea pipe_sol + pipe_res, fork() Meeseeks N=1,i=1    |
 *        | waitpid() + lee pipe_res, guarda Reporte[], repite   |
 *        | al despedirse: informe_final() + caos cleanup        |
 *        +------------------------|----------------------------+
 *                                 | fork
 *                  +--------------v----------------+
 *                  | MEESSEKS rutina_meeseeks()    |
 *                  | 1. welcome (pid,ppid,N,i)     |
 *                  | 2. lee solicitud por pipe     |
 *                  | 3. segun tipo: A/B/C          |
 *                  | 4a. A: sleep(sim), tira dado, |
 *                  |     si dificil -> crear_hijos |
 *                  |     (2 o 5, N+1, aging)       |
 *                  | 4b. B: eval_aritmetica()      |
 *                  | 4c. C: fork+execvp+wait       |
 *                  | 5. escribe Resultado por pipe |
 *                  |    al padre + despedida       |
 *                  +--------------+---------------+
 *                                 | fork x2 / x5
 *                        hijos N+1 (recursivo)
 *
 * Comunicacion IPC:
 *   - pipes (pipe()) para TODO mensaje/solicitud/resultado. NO se usa
 *     variable global para mensajes (exigencia de la tarea).
 *   - variable compartida SOLO para contadores (total_meeseeks, nivel_max)
 *     ubicada en mmap(MAP_SHARED) + sem_t pshared para exclusion mutua.
 *   - semaforos: biblioteca pthreads/semaphore.h (sem_init pshared=1,
 *     sem_wait/sem_post). Exigencia: "semaforos administrados por el SO".
 *   - terminacion de hermanos: padre detecta exito por pipe_res no
 *     bloqueante y envia SIGTERM a hermanos restantes; cada Meeseeks
 *     instala manejador SIGTERM que imprime despedida y sale. Asi se
 *     cumple "padre comunica a hermanos para que terminen y se despidan".
 *
 * Plan de contingencia dificultad 0 (anti fork-bomb):
 *   1. Dificultad 0 NUNCA tiene exito (tiro dado siempre falla).
 *   2. Tope MAX_NIVEL=6 y MAX_MESEEKS_POR_TAREA=60. Al llegar, se aborta
 *      la rama y se reporta fracaso sin mas forks.
 *   3. Aging NO aplica a d=0 (se queda en 0) para que no se "lave" a 100.
 *   4. Timeout por tarea (limite interno) + timeout global caos planetario.
 *   5. Expansion limitada: 0-45 -> 5 hijos, 45-85 -> 2 hijos, 85-100 -> 0.
 *   Todo documentado tambien en el informe PDF.
 *
 * Tiempos:
 *   - t_total  = wall clock (CLOCK_MONOTONIC) desde fork hasta wait.
 *   - t_real   = t_total - tiempo_simulacion (se resta el sleep aleatorio
 *     de simulacion, segun exige la tarea: "sin tomar en consideracion los
 *     tiempos de espera aleatorios"). Para B/C t_real ~= t_total porque no
 *     hay sleep de simulacion, solo ejecucion real.
 *
 * Caos planetario:
 *   - Alarma configurable (argv[1], default 300 s). Al dispararse, el padre
 *     hace kill(0,SIGTERM) para terminar todo su grupo y sale.
 *   - Prueba extra exigida: correr con timeout muy grande o 0 (=sin limite)
 *     para medir "tiempo adicional" en tareas dificiles. Documentar.
 * ========================================================================== */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <time.h>
#include <semaphore.h>
#include <signal.h>
#include <errno.h>
#include <math.h>
#include <ctype.h>
#include <fcntl.h>

/* ----------------------------- Configuracion -------------------------- */
#define MAX_TEXTO            512
#define MAX_TAREAS           100
#define MAX_NIVEL            6    /* Contingencia: profundidad maxima      */
#define MAX_MESEEKS_TAREA    60   /* Contingencia: tope de procesos/tarea  */
#define MAX_ARGS_EXEC        32
#define DEFAULT_CAOS_SEG     300  /* 5 minutos, base minima exigida        */
#define SIM_MIN_SEG          0.5
#define SIM_MAX_SEG          5.0

/* Tipos de solicitud */
#define TIPO_A_TEXTUAL  1
#define TIPO_B_CALCULO  2
#define TIPO_C_EXTERNO  3

/* ------------------------- Estructuras compartidas -------------------- */
/* Solo contadores van en memoria compartida (permitido por la tarea). */
typedef struct {
    sem_t mutex;            /* semaforo pshared para exclusion mutua */
    int   total_meeseeks;   /* total historico de Meeseeks creados   */
    int   nivel_max;        /* nivel N maximo alcanzado              */
} ContadoresShared;

/* Resultado que viaja por pipe hijo -> padre (nunca por global). */
typedef struct {
    int   exito;            /* 1 exito, 0 fracaso */
    pid_t solver_pid;
    pid_t solver_ppid;
    int   solver_N;
    int   solver_i;
} ResultadoPipe;

/* Reporte por tarea que guarda el Box (proceso padre). */
typedef struct {
    char  solicitud[MAX_TEXTO];
    int   tipo;
    float dificultad;       /* solo A; B/C usan -1 */
    double dur_sim;         /* sleep simulacion (A) */
    double t_total;         /* wall */
    double t_real;          /* wall - sim */
    int   exito;
    int   num_meeseeks;     /* cuantos se crearon para esta tarea */
    pid_t solver_pid;
    int   solver_N;
    int   solver_i;
} Reporte;

/* ------------------------------ Globales Box -------------------------- */
/* OJO: estas variables solo las toca el Box (padre). Los hijos se comunican
 * por pipes, como exige la tarea. */
static Reporte g_reportes[MAX_TAREAS];
static int     g_num_tareas = 0;
static ContadoresShared *g_shared = NULL;
static int     g_caos_seg = DEFAULT_CAOS_SEG;
static volatile sig_atomic_t g_caos = 0;

/* Contexto del Meeseeks en curso (solo para el manejador SIGTERM). */
static pid_t g_mi_pid  = 0;
static pid_t g_mi_ppid = 0;
static int   g_mi_N = 1;
static int   g_mi_i = 1;

/* ------------------------------ Prototipos ---------------------------- */
static double ahora_seg(void);
static void   trim_nl(char *s);
static int    es_despedida(const char *s);
static int    detectar_tipo(const char *s);
static const char *nombre_tipo(int t);
static void   mensaje_box(const char *msg);
static void   mensaje_meeseeks(const char *msg);
static void   manejador_caos(int sig);
static void   manejador_meeseeks_term(int sig);
static float  pedir_dificultad(void);
static double pedir_duracion(void);
static double duracion_aleatoria(void);
static float  dificultad_aleatoria(void);
static void   incrementar_contador(int N);
static int    leer_contador_total(void);

/* Caso B: evaluador */
static int evaluar_expresion(const char *expr, double *out, char *err, size_t errn);

/* Caso C: programa externo */
static int ejecutar_externo(const char *cmdline, int *rc_out);

/* Nucleo Meeseeks */
static int  rutina_meeseeks(char *solicitud, int tipo, float dificultad,
                            double dur_sim, int N, int i, int fd_res);
static int  casoA_intentar(float dificultad);
static float aging_dificultad(float d_padre, int num_hijos, int nivel);
static int  crear_hijos(char *solicitud, int tipo, float d_hija,
                        double dur_sim, int N, ResultadoPipe *res_out);

/* Box */
static void box_loop(void);
static void informe_final(void);

/* ======================================================================
 * Utilidades de tiempo y texto
 * ==================================================================== */
static double ahora_seg(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void trim_nl(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r')) s[--n] = '\0';
    /* quitar espacios extremos */
    char *p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
    n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n-1])) s[--n] = '\0';
}

static int es_despedida(const char *s)
{
    char tmp[MAX_TEXTO];
    strncpy(tmp, s, sizeof(tmp)-1); tmp[sizeof(tmp)-1] = '\0';
    for (char *p = tmp; *p; p++) *p = tolower((unsigned char)*p);
    return (!strcmp(tmp,"adios") || !strcmp(tmp,"adiós") || !strcmp(tmp,"bye") ||
            !strcmp(tmp,"exit") || !strcmp(tmp,"quit") || !strcmp(tmp,"chao") ||
            !strcmp(tmp,"terminar") || !strcmp(tmp,"salir"));
}

/* Deteccion de tipo:
 *  "exec: ..." o "run: ..."  -> C
 *  "calc: ..."                -> B
 *  solo caracteres matematicos -> B (autodeteccion)
 *  resto                      -> A textual */
static int detectar_tipo(const char *s)
{
    if (!strncasecmp(s, "exec:", 5) || !strncasecmp(s, "run:", 4) ||
        !strncasecmp(s, "exec ", 5) || !strncasecmp(s, "run ", 4))
        return TIPO_C_EXTERNO;
    if (!strncasecmp(s, "calc:", 5) || !strncasecmp(s, "calc ", 5))
        return TIPO_B_CALCULO;
    /* autodeteccion B: solo digitos, operadores, parentesis, comparadores */
    int solo_math = 1, tiene_dig = 0;
    for (const char *p = s; *p; p++) {
        if (isdigit((unsigned char)*p)) { tiene_dig = 1; continue; }
        if (strchr("+-*/%(). <>=!&|", *p)) continue;
        solo_math = 0; break;
    }
    if (solo_math && tiene_dig) return TIPO_B_CALCULO;
    return TIPO_A_TEXTUAL;
}

static const char *nombre_tipo(int t)
{
    return (t == TIPO_A_TEXTUAL) ? "A-textual" :
           (t == TIPO_B_CALCULO) ? "B-calculo" : "C-externo";
}

static void mensaje_box(const char *msg)
{
    printf("[BOX pid=%d] %s\n", (int)getpid(), msg);
    fflush(stdout);
}

static void mensaje_meeseeks(const char *msg)
{
    printf("[Meeseeks pid=%d ppid=%d N=%d i=%d] %s\n",
           (int)getpid(), (int)getppid(), g_mi_N, g_mi_i, msg);
    fflush(stdout);
}

/* ------------------------- Manejadores de senal ----------------------- */
static void manejador_caos(int sig)
{
    (void)sig;
    g_caos = 1;
    printf("\n[BOX pid=%d] *** CAOS PLANETARIO *** Tiempo maximo (%d s) agotado. "
           "Terminando todos los Meeseeks...\n", (int)getpid(), g_caos_seg);
    fflush(stdout);
    kill(0, SIGTERM); /* mata todo el grupo (Box + Meeseeks) */
    sleep(1);
    kill(0, SIGKILL);
    _exit(99);
}

/* Cada Meeseeks que recibe SIGTERM se despide antes de morir (exigencia). */
static void manejador_meeseeks_term(int sig)
{
    (void)sig;
    char buf[256];
    snprintf(buf, sizeof(buf),
             "Ohh no! Me avisaron que otro me resolvio / llego el caos. "
             "Me despido y desaparezco... POOF!");
    mensaje_meeseeks(buf);
    _exit(2);
}

/* ------------------- Dificultad y duracion (caso A) ------------------- */
static float dificultad_aleatoria(void)
{
    /* Distribucion uniforme 0-100 (documentada en el PDF; basta para
     * simular "suerte del Meeseeks"). */
    return (float)(rand() % 101);
}

static double duracion_aleatoria(void)
{
    double r = (double)rand() / (double)RAND_MAX; /* 0..1 */
    return SIM_MIN_SEG + r * (SIM_MAX_SEG - SIM_MIN_SEG);
}

static float pedir_dificultad(void)
{
    char buf[64];
    printf("  Dificultad: (1) ingresarla manual 0-100 | (2) suerte Meeseeks? [2]: ");
    fflush(stdout);
    if (!fgets(buf, sizeof(buf), stdin)) return dificultad_aleatoria();
    if (buf[0] == '1') {
        printf("  Ingrese dificultad (100=muy facil, 0=imposible): ");
        fflush(stdout);
        if (!fgets(buf, sizeof(buf), stdin)) return dificultad_aleatoria();
        float d = atof(buf);
        if (d < 0) d = 0;
        if (d > 100) d = 100;
        return d;
    }
    float d = dificultad_aleatoria();
    printf("  (suerte Meeseeks -> dificultad=%.2f)\n", d);
    return d;
}

static double pedir_duracion(void)
{
    char buf[64];
    printf("  Duracion simulacion: (1) manual 0.5-5 s | (2) aleatoria? [2]: ");
    fflush(stdout);
    if (!fgets(buf, sizeof(buf), stdin)) return duracion_aleatoria();
    if (buf[0] == '1') {
        printf("  Ingrese segundos (0.5-5.0): ");
        fflush(stdout);
        if (!fgets(buf, sizeof(buf), stdin)) return duracion_aleatoria();
        double d = atof(buf);
        if (d < SIM_MIN_SEG) d = SIM_MIN_SEG;
        if (d > SIM_MAX_SEG) d = SIM_MAX_SEG;
        return d;
    }
    double d = duracion_aleatoria();
    printf("  (duracion aleatoria -> %.2f s)\n", d);
    return d;
}

/* ---------------- Contadores compartidos + semaforo ------------------- */
static void incrementar_contador(int N)
{
    if (!g_shared) return;
    sem_wait(&g_shared->mutex);
    g_shared->total_meeseeks++;
    if (N > g_shared->nivel_max) g_shared->nivel_max = N;
    sem_post(&g_shared->mutex);
}

static int leer_contador_total(void)
{
    int v = 0;
    if (!g_shared) return 0;
    sem_wait(&g_shared->mutex);
    v = g_shared->total_meeseeks;
    sem_post(&g_shared->mutex);
    return v;
}

/* ======================================================================
 * CASO B: mini evaluador aritmetico/logico.
 * Gramatica (precedencia):  or(||) > and(&&) > comparacion(== != < > <= >=)
 *  > suma(+ -) > producto(* / %) > unario(- ! ) > primario(num, (expr)).
 * Un solo Meeseeks lo ejecuta (exigencia). Retorna 1 ok / 0 error.
 * ==================================================================== */
typedef struct { const char *p; char err[128]; int fallo; } Parser;

static void ps_espacios(Parser *s){ while (isspace((unsigned char)*s->p)) s->p++; }
static double ps_or(Parser *s);

static double ps_primario(Parser *s)
{
    ps_espacios(s);
    if (*s->p == '(') {
        s->p++;
        double v = ps_or(s);
        ps_espacios(s);
        if (*s->p == ')') s->p++;
        else { snprintf(s->err,sizeof(s->err),"falta ')'"); s->fallo=1; }
        return v;
    }
    char *end = NULL;
    double v = strtod(s->p, &end);
    if (end == s->p) { snprintf(s->err,sizeof(s->err),"token invalido en '%s'",s->p); s->fallo=1; return 0; }
    s->p = end;
    return v;
}
static double ps_unario(Parser *s)
{
    ps_espacios(s);
    if (*s->p == '-') { s->p++; return -ps_unario(s); }
    if (*s->p == '!') { s->p++; return !ps_unario(s); }
    if (*s->p == '+') { s->p++; return ps_unario(s); }
    return ps_primario(s);
}
static double ps_producto(Parser *s)
{
    double v = ps_unario(s);
    for (;;) {
        ps_espacios(s);
        if (*s->p == '*') { s->p++; v *= ps_unario(s); }
        else if (*s->p == '/') { s->p++; double d=ps_unario(s);
            if (d==0){ snprintf(s->err,sizeof(s->err),"division por cero"); s->fallo=1; return 0;} v/=d; }
        else if (*s->p == '%') { s->p++; double d=ps_unario(s);
            if (d==0){ snprintf(s->err,sizeof(s->err),"modulo por cero"); s->fallo=1; return 0;} v=fmod(v,d); }
        else return v;
    }
}
static double ps_suma(Parser *s)
{
    double v = ps_producto(s);
    for (;;) {
        ps_espacios(s);
        if (*s->p == '+') { s->p++; v += ps_producto(s); }
        else if (*s->p == '-') { s->p++; v -= ps_producto(s); }
        else return v;
    }
}
static double ps_comp(Parser *s)
{
    double v = ps_suma(s);
    for (;;) {
        ps_espacios(s);
        if (!strncmp(s->p,"==",2)){ s->p+=2; v=(v==ps_suma(s)); }
        else if (!strncmp(s->p,"!=",2)){ s->p+=2; v=(v!=ps_suma(s)); }
        else if (!strncmp(s->p,"<=",2)){ s->p+=2; v=(v<=ps_suma(s)); }
        else if (!strncmp(s->p,">=",2)){ s->p+=2; v=(v>=ps_suma(s)); }
        else if (*s->p=='<'){ s->p++; v=(v<ps_suma(s)); }
        else if (*s->p=='>'){ s->p++; v=(v>ps_suma(s)); }
        else return v;
    }
}
static double ps_and(Parser *s)
{
    double v = ps_comp(s);
    for (;;) {
        ps_espacios(s);
        if (!strncmp(s->p,"&&",2)){ s->p+=2; v=((v!=0)&&(ps_comp(s)!=0)); }
        else return v;
    }
}
static double ps_or(Parser *s)
{
    double v = ps_and(s);
    for (;;) {
        ps_espacios(s);
        if (!strncmp(s->p,"||",2)){ s->p+=2; v=((v!=0)||(ps_comp(s)!=0)); }
        else return v;
    }
}

static int evaluar_expresion(const char *expr, double *out, char *err, size_t errn)
{
    Parser s; s.p = expr; s.fallo = 0; s.err[0] = '\0';
    double v = ps_or(&s);
    ps_espacios(&s);
    if (s.fallo) { snprintf(err,errn,"%s",s.err); return 0; }
    if (*s.p != '\0') { snprintf(err,errn,"texto sobrante '%s'",s.p); return 0; }
    *out = v;
    return 1;
}

/* ======================================================================
 * CASO C: ejecucion de programa externo.
 * Separa por espacios (simple), hace fork+execvp y espera con waitpid.
 * Exito <=> WIFEXITED && WEXITSTATUS==0 (convencion UNIX).
 * ==================================================================== */
static int ejecutar_externo(const char *cmdline, int *rc_out)
{
    char buf[MAX_TEXTO];
    strncpy(buf, cmdline, sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    char *argv[MAX_ARGS_EXEC+1];
    int n = 0;
    char *tok = strtok(buf, " \t");
    while (tok && n < MAX_ARGS_EXEC) { argv[n++] = tok; tok = strtok(NULL, " \t"); }
    argv[n] = NULL;
    if (n == 0) return 0;

    pid_t p = fork();
    if (p < 0) return 0;
    if (p == 0) {
        execvp(argv[0], argv);
        fprintf(stderr, "[Meeseeks pid=%d] exec fallo: %s\n",
                (int)getpid(), strerror(errno));
        _exit(127);
    }
    int st = 0;
    while (waitpid(p, &st, 0) < 0 && errno == EINTR) ;
    int ok = (WIFEXITED(st) && WEXITSTATUS(st) == 0);
    if (rc_out) *rc_out = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    return ok;
}

/* ======================================================================
 * CASO A: probabilidad + aging.
 * Propuesta creativa: el dado es uniforme 0-100 comparado contra la
 * dificultad. d=100 siempre gana, d=0 nunca gana. Si pierde y esta en
 * rango dificil, pide ayuda (ramifica). El aging diluye la dificultad:
 *   d_hija = d_padre + (100-d_padre)/num_hijos + 2*N ... tope 99.
 * Asi cada nivel es mas facil, garantizando convergencia salvo d=0
 * (que se congela en 0 a proposito para simular "imposible").
 * ==================================================================== */
static int casoA_intentar(float dificultad)
{
    if (dificultad <= 0.0f) return 0;          /* imposible: nunca gana */
    if (dificultad >= 100.0f) return 1;        /* trivial: siempre gana */
    double tiro = (double)rand() / (double)RAND_MAX * 100.0;
    return (tiro <= (double)dificultad) ? 1 : 0;
}

static float aging_dificultad(float d_padre, int num_hijos, int nivel)
{
    if (d_padre <= 0.0f) return 0.0f; /* imposible no se diluye (contingencia) */
    float d = d_padre + (100.0f - d_padre) / (float)num_hijos + 2.0f * nivel;
    if (d > 99.0f) d = 99.0f;
    return d;
}

/* Crea 2 o 5 hijos en paralelo con pipes de resultado + control SIGTERM.
 * Retorna 1 si algun subarbol tuvo exito (y llena res_out con el solver). */
static int crear_hijos(char *solicitud, int tipo, float d_hija,
                       double dur_sim, int N, ResultadoPipe *res_out)
{
    int num = (d_hija <= 45.0f) ? 5 : 2; /* segun enunciado: 0-45->5, 45-85->2 */
    /* Contingencia: recortar si nos acercamos al tope por tarea */
    int usados = leer_contador_total();
    if (usados + num > MAX_MESEEKS_TAREA) {
        num = MAX_MESEEKS_TAREA - usados;
        if (num <= 0) {
            mensaje_meeseeks("Tope MAX_MESEEKS alcanzado, no puedo crear mas ayuda. Fracaso esta rama.");
            return 0;
        }
    }
    if (N + 1 > MAX_NIVEL) {
        mensaje_meeseeks("Tope MAX_NIVEL alcanzado, no ramifico mas. Fracaso esta rama.");
        return 0;
    }

    char buf[256];
    snprintf(buf, sizeof(buf),
             "Uff! Esta dificil (d=%.2f). Voy a pedir ayuda a %d Meeseeks del nivel %d...",
             d_hija, num, N+1);
    mensaje_meeseeks(buf);

    int   *res_fds = calloc(num, sizeof(int));
    pid_t *pids    = calloc(num, sizeof(pid_t));
    int launched = 0;

    for (int k = 0; k < num; k++) {
        int fd[2];
        if (pipe(fd) < 0) { perror("pipe"); break; }
        /* no-bloqueante para poder detectar exito temprano */
        fcntl(fd[0], F_SETFL, O_NONBLOCK);
        fflush(stdout);
        pid_t p = fork();
        if (p < 0) { perror("fork"); close(fd[0]); close(fd[1]); break; }
        if (p == 0) {
            /* hijo k: cierra lectura, ejecuta rutina con N+1, i=k+1 */
            close(fd[0]);
            int r = rutina_meeseeks(solicitud, tipo, d_hija, dur_sim,
                                   N+1, k+1, fd[1]);
            close(fd[1]);
            _exit(r ? 0 : 1);
        }
        /* padre: cierra escritura, guarda lectura */
        close(fd[1]);
        res_fds[launched] = fd[0];
        pids[launched] = p;
        launched++;
    }

    /* Esperar con sondeo: si uno avisa exito, matar al resto (aviso real). */
    int exito = 0;
    ResultadoPipe ganador; memset(&ganador, 0, sizeof(ganador));
    int *done = calloc(launched, sizeof(int));
    int pendientes = launched;

    while (pendientes > 0) {
        for (int k = 0; k < launched; k++) {
            if (done[k]) continue;
            /* ¿llego resultado por el pipe? */
            ResultadoPipe r;
            ssize_t n = read(res_fds[k], &r, sizeof(r));
            if (n == sizeof(r)) {
                done[k] = 1; pendientes--;
                close(res_fds[k]);
                int st; waitpid(pids[k], &st, 0);
                if (r.exito && !exito) {
                    exito = 1; ganador = r;
                    /* Avisar a hermanos restantes: SIGTERM (se despiden) */
                    char m[256];
                    snprintf(m, sizeof(m),
                             "Mi hijo pid=%d resolvio! Avisando a sus %d hermanos que terminen...",
                             (int)r.solver_pid, pendientes);
                    mensaje_meeseeks(m);
                    for (int j = 0; j < launched; j++)
                        if (!done[j]) kill(pids[j], SIGTERM);
                }
            } else {
                /* ¿el hijo ya murio sin escribir? cosecharlo no bloqueante */
                int st; pid_t w = waitpid(pids[k], &st, WNOHANG);
                if (w == pids[k]) {
                    /* drenar posible resultado pendiente */
                    ResultadoPipe r2;
                    ssize_t n2 = read(res_fds[k], &r2, sizeof(r2));
                    done[k] = 1; pendientes--;
                    close(res_fds[k]);
                    if (n2 == sizeof(r2) && r2.exito && !exito) {
                        exito = 1; ganador = r2;
                        for (int j = 0; j < launched; j++)
                            if (!done[j]) kill(pids[j], SIGTERM);
                    }
                }
            }
        }
        if (pendientes > 0) { struct timespec t={0, 50*1000*1000}; nanosleep(&t,NULL); }
    }

    free(res_fds); free(pids); free(done);
    if (exito && res_out) *res_out = ganador;
    return exito;
}

/* ======================================================================
 * Rutina principal de CADA Meeseeks (mismo codigo para todos los hijos).
 * fd_res: pipe hijo->padre donde informa exito/fracaso. -1 si es raiz Box
 *         (la raiz la maneja box_loop con su propio pipe).
 * Retorna 1 exito / 0 fracaso (tambien lo escribe por el pipe).
 * ==================================================================== */
static int rutina_meeseeks(char *solicitud, int tipo, float dificultad,
                           double dur_sim, int N, int i, int fd_res)
{
    g_mi_pid = getpid(); g_mi_ppid = getppid(); g_mi_N = N; g_mi_i = i;
    signal(SIGTERM, manejador_meeseeks_term);
    srand((unsigned)(time(NULL) ^ (getpid()*2654435761u)));

    incrementar_contador(N);

    /* Bienvenida exigida */
    printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (pid=%d, ppid=%d, N=%d, i=%d)\n",
           (int)getpid(), (int)getppid(), N, i);
    fflush(stdout);

    {
        char m[MAX_TEXTO+64];
        snprintf(m, sizeof(m), "Recibi por pipe la solicitud [%s] tipo=%s d=%.2f sim=%.2fs. Voy a intentarlo!",
                 solicitud, nombre_tipo(tipo), dificultad, dur_sim);
        mensaje_meeseeks(m);
    }

    int exito = 0;
    ResultadoPipe yo; memset(&yo,0,sizeof(yo));
    yo.solver_pid = getpid(); yo.solver_ppid = getppid();
    yo.solver_N = N; yo.solver_i = i;

    if (tipo == TIPO_B_CALCULO) {
        /* ---- Caso B: un solo Meeseeks, sin ramificar ---- */
        const char *expr = solicitud;
        if (!strncasecmp(expr,"calc:",5)) expr += 5;
        else if (!strncasecmp(expr,"calc ",5)) expr += 5;
        while (*expr && isspace((unsigned char)*expr)) expr++;
        double val = 0; char err[128] = "";
        mensaje_meeseeks("Soy calculador, no necesito ayuda. Evaluando...");
        struct timespec t={0, 300*1000*1000}; nanosleep(&t,NULL); /* trabajo */
        if (evaluar_expresion(expr, &val, err, sizeof(err))) {
            char m[256]; snprintf(m,sizeof(m),"Calculo OK: %s = %g. Existencia cumplida!",expr,val);
            mensaje_meeseeks(m);
            exito = 1;
        } else {
            char m[256]; snprintf(m,sizeof(m),"Calculo FALLO: %s (error: %s)",expr,err);
            mensaje_meeseeks(m);
            exito = 0;
        }
    }
    else if (tipo == TIPO_C_EXTERNO) {
        /* ---- Caso C: ejecuta programa externo y espera rc ---- */
        const char *cmd = solicitud;
        if (!strncasecmp(cmd,"exec:",5)) cmd += 5;
        else if (!strncasecmp(cmd,"run:",4)) cmd += 4;
        else if (!strncasecmp(cmd,"exec ",5)) cmd += 5;
        while (*cmd && isspace((unsigned char)*cmd)) cmd++;
        char m[600]; snprintf(m,sizeof(m),"Voy a ejecutar programa externo: '%s' y esperar su rc...",cmd);
        mensaje_meeseeks(m);
        int rc = -1;
        exito = ejecutar_externo(cmd, &rc);
        snprintf(m,sizeof(m),"Programa externo termino rc=%d -> %s.",rc,exito?"EXITO":"FALLO");
        mensaje_meeseeks(m);
    }
    else {
        /* ---- Caso A: simulacion + probabilidad + posible ramificacion ---- */
        struct timespec ts;
        ts.tv_sec  = (time_t)dur_sim;
        ts.tv_nsec = (long)((dur_sim - ts.tv_sec) * 1e9);
        nanosleep(&ts, NULL); /* tiempo de simulacion (se resta de t_real) */

        if (dificultad <= 0.0f) {
            mensaje_meeseeks("Esta tarea es IMPOSIBLE (d=0). Lo intentare con ayuda pero jamas lo lograre...");
        }

        if (casoA_intentar(dificultad)) {
            char m[256];
            snprintf(m,sizeof(m),"LO LOGRE! Solicitud '%s' completada por mi. Gracias por servirle!",solicitud);
            mensaje_meeseeks(m);
            exito = 1;
        } else if (dificultad < 85.01f) {
            /* Dificil: pedir ayuda. Aging diluye dificultad para hijos. */
            int nh = (dificultad <= 45.0f) ? 5 : 2;
            float d_hija = aging_dificultad(dificultad, nh, N);
            char m[256];
            snprintf(m,sizeof(m),"Solo no pude (d=%.2f). Dificultad diluida para hijos: %.2f (aging).",dificultad,d_hija);
            mensaje_meeseeks(m);
            /* duracion hija un poco menor (tarea subdividida) */
            double dur_hija = dur_sim * 0.8;
            if (dur_hija < SIM_MIN_SEG) dur_hija = SIM_MIN_SEG;
            ResultadoPipe sub; memset(&sub,0,sizeof(sub));
            int ok = crear_hijos(solicitud, tipo, d_hija, dur_hija, N, &sub);
            if (ok) {
                char m2[256];
                snprintf(m2,sizeof(m2),"Mi subarbol resolvio! El heroe fue pid=%d (N=%d,i=%d). Propago exito hacia arriba.",
                         (int)sub.solver_pid, sub.solver_N, sub.solver_i);
                mensaje_meeseeks(m2);
                yo = sub; /* propaga identidad del solver original */
                exito = 1;
            } else {
                mensaje_meeseeks("Ni con ayuda pudimos... Fracase en esta rama.");
                exito = 0;
            }
        } else {
            mensaje_meeseeks("Era facil (85-100) pero el dado me dijo que no. Fracase sin pedir ayuda.");
            exito = 0;
        }
    }

    /* Despedida exigida + aviso por pipe al padre */
    {
        char m[256];
        if (exito) snprintf(m,sizeof(m),"Agradecido de servirle! Me despido y desaparezco... POOF! (EXITO)");
        else       snprintf(m,sizeof(m),"No pude cumplir... Me despido y desaparezco... POOF! (FALLO)");
        mensaje_meeseeks(m);
    }
    if (fd_res >= 0) {
        ResultadoPipe r = yo;
        r.exito = exito;
        if (exito && r.solver_pid == 0) {
            r.solver_pid=getpid(); r.solver_ppid=getppid();
            r.solver_N=N; r.solver_i=i;
        }
        /* reintento de escritura (pipe no bloqueante del padre no afecta) */
        ssize_t w = write(fd_res, &r, sizeof(r));
        (void)w;
    }
    return exito;
}

/* ======================================================================
 * BOX: loop principal
 * ==================================================================== */
static void box_loop(void)
{
    char linea[MAX_TEXTO];

    mensaje_box("Caja Mr. Meeseeks iniciada. Mireme! (Look at meee!)");
    printf("[BOX] Comandos: texto libre=A | 'calc: expr'=B | 'exec: prog args'=C | adios/bye/exit=salir\n");
    printf("[BOX] Caos planetario en %d s. Tareas max %d.\n", g_caos_seg, MAX_TAREAS);
    fflush(stdout);

    while (g_num_tareas < MAX_TAREAS) {
        printf("\n[BOX pid=%d] Solicitud #%d > ", (int)getpid(), g_num_tareas+1);
        fflush(stdout);
        if (!fgets(linea, sizeof(linea), stdin)) { printf("\n"); break; }
        trim_nl(linea);
        if (linea[0] == '\0') continue;
        if (es_despedida(linea)) break;

        int tipo = detectar_tipo(linea);
        float dificultad = -1;
        double dur_sim = 0;
        if (tipo == TIPO_A_TEXTUAL) {
            dificultad = pedir_dificultad();
            dur_sim    = pedir_duracion();
        }

        /* Pipes Box<->Meeseeks inicial: solicitud (Box->hijo) y resultado */
        int pipe_sol[2], pipe_res[2];
        if (pipe(pipe_sol) < 0 || pipe(pipe_res) < 0) { perror("pipe"); continue; }
        fcntl(pipe_res[0], F_SETFL, O_NONBLOCK);

        double t0 = ahora_seg();
        int total_antes = leer_contador_total();

        fflush(stdout);
        pid_t p = fork();
        if (p < 0) { perror("fork"); close(pipe_sol[0]); close(pipe_sol[1]); close(pipe_res[0]); close(pipe_res[1]); continue; }

        if (p == 0) {
            /* ---- Hijo: Meeseeks inicial N=1,i=1 ---- */
            close(pipe_sol[1]); close(pipe_res[0]);
            /* Lee solicitud por pipe (exigencia: mensaje privado via pipe) */
            char sol[MAX_TEXTO] = "";
            ssize_t n = 0, acc = 0;
            /* el Box ya escribio y cerro; lectura simple bloqueante temporal */
            fcntl(pipe_sol[0], F_SETFL, 0);
            while ((n = read(pipe_sol[0], sol+acc, sizeof(sol)-1-acc)) > 0) {
                acc += n;
                if (acc >= (ssize_t)sizeof(sol)-1) break;
            }
            sol[acc] = '\0';
            close(pipe_sol[0]);
            if (acc == 0) { strncpy(sol, linea, sizeof(sol)-1); sol[sizeof(sol)-1]='\0'; }
            int r = rutina_meeseeks(sol, tipo, dificultad, dur_sim, 1, 1, pipe_res[1]);
            close(pipe_res[1]);
            _exit(r ? 0 : 1);
        }

        /* ---- Padre Box ---- */
        close(pipe_sol[0]); close(pipe_res[1]);
        /* envia solicitud por pipe y cierra escritura para EOF */
        ssize_t _w = write(pipe_sol[1], linea, strlen(linea));
        (void)_w;
        close(pipe_sol[1]);

        char m0[600];
        snprintf(m0,sizeof(m0),"Cree Meeseeks inicial pid=%d para '%s' tipo=%s. Esperando resultado...",
                 (int)p, linea, nombre_tipo(tipo));
        mensaje_box(m0);

        /* Espera + lee resultado (con reintentos porque es no-bloqueante) */
        int st = 0;
        while (waitpid(p, &st, 0) < 0 && errno == EINTR) ;
        ResultadoPipe res; memset(&res,0,sizeof(res));
        ssize_t nr = -1;
        for (int t = 0; t < 20 && nr != sizeof(res); t++) {
            nr = read(pipe_res[0], &res, sizeof(res));
            if (nr != sizeof(res)) { struct timespec z={0,20*1000*1000}; nanosleep(&z,NULL); }
        }
        close(pipe_res[0]);
        double t1 = ahora_seg();
        int total_desp = leer_contador_total();
        int usados = total_desp - total_antes;

        int exito_box = 0;
        if (nr == sizeof(res)) exito_box = res.exito;
        else exito_box = (WIFEXITED(st) && WEXITSTATUS(st) == 0);

        double t_total = t1 - t0;
        double t_real  = t_total - ((tipo==TIPO_A_TEXTUAL)?dur_sim:0.0);
        if (t_real < 0) t_real = 0;

        /* Guarda reporte */
        Reporte *rp = &g_reportes[g_num_tareas++];
        snprintf(rp->solicitud, sizeof(rp->solicitud), "%s", linea);
        rp->tipo = tipo; rp->dificultad = dificultad; rp->dur_sim = dur_sim;
        rp->t_total = t_total; rp->t_real = t_real;
        rp->exito = exito_box; rp->num_meeseeks = usados;
        rp->solver_pid = res.solver_pid; rp->solver_N = res.solver_N; rp->solver_i = res.solver_i;

        char m1[700];
        if (exito_box)
            snprintf(m1,sizeof(m1),"Tarea #%d COMPLETADA en %.2fs (real %.2fs, %d Meeseeks). Heroe pid=%d (N=%d,i=%d).",
                     g_num_tareas, t_total, t_real, usados,
                     (int)res.solver_pid, res.solver_N, res.solver_i);
        else
            snprintf(m1,sizeof(m1),"Tarea #%d NO completada en %.2fs (real %.2fs, %d Meeseeks).",
                     g_num_tareas, t_total, t_real, usados);
        mensaje_box(m1);
        printf("[BOX] Control devuelto al Box. Listo para la siguiente solicitud.\n");
        fflush(stdout);
    }

    informe_final();
}

/* Informe exigido al cerrar el Box */
static void informe_final(void)
{
    printf("\n================ INFORME FINAL BOX =================\n");
    printf("Tareas solicitadas : %d\n", g_num_tareas);
    int ok = 0;
    for (int k = 0; k < g_num_tareas; k++) {
        Reporte *r = &g_reportes[k];
        if (r->exito) ok++;
        printf(" #%02d [%s] '%s' d=%.2f | total=%.2fs real=%.2fs | %s | meeseeks=%d",
               k+1, nombre_tipo(r->tipo), r->solicitud, r->dificultad,
               r->t_total, r->t_real, r->exito?"OK":"FALLO", r->num_meeseeks);
        if (r->exito) printf(" | heroe pid=%d N=%d i=%d", (int)r->solver_pid, r->solver_N, r->solver_i);
        printf("\n");
    }
    printf("Completadas: %d | No completadas: %d\n", ok, g_num_tareas - ok);
    if (g_shared) {
        sem_wait(&g_shared->mutex);
        printf("Total Meeseeks creados: %d | Nivel maximo: %d\n",
               g_shared->total_meeseeks, g_shared->nivel_max);
        sem_post(&g_shared->mutex);
    }
    printf("====================================================\n");
    printf("[BOX pid=%d] Gracias! Adios! (Box termina)\n", (int)getpid());
    fflush(stdout);
}

/* ======================================================================
 * main()
 * ==================================================================== */
int main(int argc, char *argv[])
{
    /* stdout sin buffer para no duplicar mensajes tras fork */
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc >= 2) {
        g_caos_seg = atoi(argv[1]);
        if (g_caos_seg < 0) g_caos_seg = 0; /* 0 = sin limite (para prueba extra) */
    }

    /* Memoria compartida para contadores + semaforo pshared */
    g_shared = mmap(NULL, sizeof(*g_shared), PROT_READ|PROT_WRITE,
                    MAP_SHARED|MAP_ANONYMOUS, -1, 0);
    if (g_shared == MAP_FAILED) { perror("mmap"); return 1; }
    memset(g_shared, 0, sizeof(*g_shared));
    if (sem_init(&g_shared->mutex, 1, 1) != 0) { perror("sem_init"); return 1; }

    /* Caos planetario: alarma global */
    if (g_caos_seg > 0) {
        struct sigaction sa; memset(&sa,0,sizeof(sa));
        sa.sa_handler = manejador_caos;
        sigaction(SIGALRM, &sa, NULL);
        alarm(g_caos_seg);
    }

    char hdr[128];
    snprintf(hdr,sizeof(hdr),"Bienvenido! Timeout caos=%d s (0=sin limite).",
             g_caos_seg);
    mensaje_box(hdr);

    box_loop();

    if (g_caos_seg > 0) alarm(0);
    sem_destroy(&g_shared->mutex);
    munmap(g_shared, sizeof(*g_shared));
    return 0;
}
