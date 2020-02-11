/* archivo: analogh2.c
Autores:
Sergio Andrés Mejía Tovar
Santiago Palacios Loaiza
Julian David Parada Galvis
Objetivo: Generar un analizador de logs con formato específico, usando hilos para dividir el archivo con la ayuda semaforos y señales.
Funciones del programa:

static void handler(int signum)
int mgetline (char *line, int max, FILE *f)
void asignar(struct Log *miLOG, long AUX_n_job,	..., long AUX_think_time_last_job, int pos)
int compare(int num)
void floatToChar(float num, char* res)
void* mapper(struct Parameter* param)
void* reducer(struct Parameter *param)
void imprimir(int inicial, int fiinal)
int sumResults(int n_reducers)
int printMenu()
int makeConsult(int lineas, int n_mappers, int n_reducers)
int initializeConsult(int lineas, int n_mappers, int n_reducers)
void killThreads(pthread_t* p_id_map, pthread_t* p_id_red, int n_mappers, int n_reducers)
int main(int argc, char *argv[])

Fecha de finalización: 6 de mayo de 2019
*/

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <wait.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <semaphore.h>
#include "log.h"
#include "parameters2.h"
#include "result.h"

struct Log *miLOG;

sem_t *master_init;

struct Result **results;
int *tams;
sem_t *m_write;
sem_t *n_sem_mappers;
sem_t *e_sem_mappers;

int *sumas;
int *count_neg;
int tam_sumas;
sem_t r_write;
sem_t n_sem_reducers;
sem_t e_sem_reducers;

int columna;
char* criterio;
int valor;

/*
Función: handler
Autores: S. Mejia, S. Palacios, J. Parada
Parámetros de Entrada: Entero obligatorio para el funcionamiento de la funcion.
Valor de salida: No tiene
Descripción: Función asignada a la señal SIGUSR1 para la terminacion de los hilos.
*/
static void handler(int signum)
{
  pthread_exit(NULL);
}

/*
Función: mgetline
Autores: Mariela Curiel
Parámetros de Entrada: Cadena de caracteres que guardará la lectura de una
línea de un archivo dado en FILE *f y su tamaño máximo max
Valor de salida: Tamaño de la línea leída
Descripción: Lee la siguiente lińea de un archivo dado y lo guarda en la cadena de caracteres dada.
*/
int mgetline (char *line, int max, FILE *f)
{
  if (fgets(line, max, f)== NULL)
  return(0);
  else return(strlen(line));
}

/*
Función: asignar
Autores: S. Mejia, S. Palacios, J. Parada
Parámetros de Entrada: Valores a guardar en el arreglo en la posición dada en pos.
Valor de salida: No tiene
Descripción: Recibe un arreglo de Logs y una posición. Guarda los datos dados en el
arreglo en la posición dada en los datos correspondientes.
*/
void asignar(long AUX_n_job,	long AUX_submit_time,	long AUX_waiting_time,
  long AUX_executing_time,	long AUX_n_procesors,	float AUX_cpu_avg_time,	float AUX_memory_used,
  long AUX_n_asked_procesors,	long AUX_asked_time,	long AUX_asked_memory,	long AUX_status,	long AUX_id_user,
  long AUX_id_group,	long AUX_id_application,	long AUX_n_queue,	long AUX_n_partition,
  long AUX_last_job,	long AUX_think_time_last_job, int pos)
  {
    /*Guarda en cada posicion del arreglo los datos- Note los farray entre las posiciones 4 y 7*/
    miLOG[pos].larray[0] = AUX_n_job;
    miLOG[pos].larray[1] = AUX_submit_time;
    miLOG[pos].larray[2] = AUX_waiting_time;
    miLOG[pos].larray[3] = AUX_executing_time;
    miLOG[pos].larray[4] = AUX_n_procesors;
    miLOG[pos].farray[0] = AUX_cpu_avg_time;
    miLOG[pos].farray[1] = AUX_memory_used;
    miLOG[pos].larray[7] = AUX_n_asked_procesors;
    miLOG[pos].larray[8] = AUX_asked_time;
    miLOG[pos].larray[9] = AUX_asked_memory;
    miLOG[pos].larray[10] = AUX_status;
    miLOG[pos].larray[11] = AUX_id_user;
    miLOG[pos].larray[12] = AUX_id_group;
    miLOG[pos].larray[13] = AUX_id_application;
    miLOG[pos].larray[14] = AUX_n_queue;
    miLOG[pos].larray[15] = AUX_n_partition;
    miLOG[pos].larray[16] = AUX_last_job;
    miLOG[pos].larray[17] = AUX_think_time_last_job;
  }

  /*
  Función: compare
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada:
  Valor de salida: Entero con la respuesta, siendo 1 true y 0 false
  Descripción: Recibe un número y lo compara con el valor dado teniendo en cuenta el criterio enviado.
  */
  int compare(int num)
  {
    if(strcmp(criterio,">") == 0){
      if(num > valor){
        return 1;
      }
    }
    else if(strcmp(criterio,"<") == 0){
      if(num < valor){
        return 1;
      }
    }

    else if(strcmp(criterio,"<=") == 0){
      if(num <= valor){
        return 1;
      }
    }

    else if(strcmp(criterio,">=") == 0){
      if(num >= valor){
        return 1;
      }
    }

    else if(strcmp(criterio,"=") == 0){
      if(num == valor){
        return 1;
      }
    }
    return 0;
  }

  /*
  Función: floatToChar
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: Número flotante y un apuntador a una cadena de caracteres
  Valor de salida: No tiene
  Descripción: Convierte un número flotante dado en su representación en cadena de caracteres.
  */
  void floatToChar(float num, char* res)
  {
    float myFloat = num;
    int ret;

    ret = snprintf(res, sizeof(res)*2, "%f", myFloat);

    if(ret < 0)
    {
      perror("snprintf: ");
      exit(1);
    }
  }

  /*
  Función: mapper
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a una estructura de los parametros de un hilo/mapper.
  Valor de salida: No tiene
  Descripción: Recorre el arreglo de Logs en el intervalo [inicial, final] deado en la estructura que le llega, buscando aquellos que corresponden
  con la consulta (utilizando la función compare).Guarda los resultados en la seccion del arreglo global de resultados correspondiente al mapper.
  Todo esto respetando la teoria de semaforos basandose en el algoritmo de productor/consumidor asumiendo el rol de productor.
  */
  void* mapper(struct Parameter* param)
  {
    int i, j, ret;
    int res_comp;
    char datos[30];
    int fila;
    char *result;
    Result res;
    static sigset_t mask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);

    if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0) {
      perror("pthread_sigmask");
      exit(1);
    }

    while(1)
    {
      if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) != 0) {
        perror("pthread_sigmask");
        exit(1);
      }

      sem_wait(&master_init[param->id_m]);
      for(i = param->inicial; i <= param->fiinal; i++)
      {
        res_comp = 0;
        if(columna == 6 || columna == 7)
        {
          res_comp = compare(miLOG[i].farray[columna - 6]);
          if(res_comp == 1)
          {
            fila = i;
            result = (char *)malloc(sizeof(datos)*strlen(datos));
            floatToChar(miLOG[i].farray[columna-6], result);
          }
        }
        else
        {
          res_comp = compare(miLOG[i].larray[columna-1]);
          if(res_comp == 1)
          {
            fila = i+1;
            ret = snprintf(datos, sizeof(datos),"%ld",miLOG[i].larray[columna-1]);
            if(ret < 0)
            {
              perror("snprintf: ");
              pthread_exit(NULL);
            }
            result = (char *)malloc(sizeof(datos)*strlen(datos));
            strcpy(result,datos);
          }
        }

        if(res_comp)
        {
          sem_wait(&e_sem_mappers[param->id_r]);
          sem_wait(&m_write[param->id_r]);
          res.fila = fila;
          res.result = (char *)malloc(sizeof(result)*strlen(result));
          strcpy(res.result,result);
          results[param->id_r][tams[param->id_r]] = res;
          tams[param->id_r]++;
          sem_post(&m_write[param->id_r]);
          sem_post(&n_sem_mappers[param->id_r]);
        }
      }
      sem_wait(&e_sem_mappers[param->id_r]);
      sem_wait(&m_write[param->id_r]);
      res.fila = -1;
      strcpy(res.result,"-1");
      results[param->id_r][tams[param->id_r]] = res;
      tams[param->id_r]++;
      sem_post(&m_write[param->id_r]);
      sem_post(&n_sem_mappers[param->id_r]);
    }
    pthread_exit(NULL);
  }

  /*
  Función: reducer
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a una estructura de los parametros de un hilo/mapper.
  Valor de salida: No tiene
  Descripción: Recorre el arreglo de resultados global en las posiciones que le corresponden contando los tamaños de los resultados del mapper.
  Todo esto respetando la teoria de semaforos basandose en el algoritmo de productor/consumidor asumiendo el rol de consumidor.
  */
  void* reducer(struct Parameter *param)
  {
    int i, n_neg;
    int valor;
    Result resultado;
    valor = 0;
    n_neg = param->num_thr;
    static sigset_t mask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);

    if (pthread_sigmask(SIG_BLOCK, &mask, NULL) != 0) {
      perror("pthread_sigmask");
      exit(1);
    }
    while(1)
    {
      if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL) != 0) {
        perror("pthread_sigmask");
        exit(1);
      }
      sem_wait(&n_sem_mappers[param->id_r]);
      sem_wait(&m_write[param->id_r]);
      resultado = results[param->id_r][0];
      for(i = 0; i < tams[param->id_r]-1; i++)
      {
        results[param->id_r][i] = results[param->id_r][i+1];
      }
      if(i+1 < BUFFER_SIZE)
      {
        results[param->id_r][i] = results[param->id_r][i+1];
      }
      tams[param->id_r]--;
      if(resultado.fila == -1)
      {
        count_neg[param->id_r]++;
        if(count_neg[param->id_r] == n_neg)
        {
          sem_wait(&e_sem_reducers);
          sem_wait(&r_write);
          sumas[tam_sumas] = valor;
          valor = 0;
          tam_sumas++;
          sem_post(&r_write);
          sem_post(&n_sem_reducers);

          sem_wait(&e_sem_reducers);
          sem_wait(&r_write);
          sumas[tam_sumas] = -1;
          tam_sumas++;
          sem_post(&r_write);
          sem_post(&n_sem_reducers);
        }
      }
      else
      {
        valor++;
      }
      sem_post(&m_write[param->id_r]);
      sem_post(&e_sem_mappers[param->id_r]);
    }
    pthread_exit(NULL);
  }

  /*
  Función: imprimir
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: Intervalo [inicial,final] del arreglo
  Valor de salida: No tiene
  Descripción: Imprime dos valores del log en el intervalo dado. Funcion auxiliar usada para
  comprobar lectura de archivos.
  */
  void imprimir(int inicial, int fiinal)
  {
    int i;
    printf("Logs:\n");
    for(i=inicial; i <= fiinal; i++)
    {
      printf("%ld %ld\n", miLOG[i].larray[0], miLOG[i].larray[4]);
    }
  }

  /*
  Función: sumResults
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: cantidad de reducers
  Valor de salida: Entero con la suma de los valores de los reducers
  Descripción: Lee el arreglo de resultados escrito por los reducers para
  ir sumando la cantidad final de resultados.
  */
  int sumResults(int n_reducers)
  {
    int i, suma = 0;
    for(i = 0; i < n_reducers; i++)
    suma += sumas[i];
    return suma;
  }

  /*
  Función: printMenu
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: No tiene
  Valor de salida: Entero con la opción seleccionada
  Descripción: Imprime los valores del menú y lee la respuesta del usuario
  */
  int printMenu()
  {
    int opcion;
    printf("----------Generador de consultas sobre Logs----------\n");
    printf("   1. Realizar consulta\n");
    printf("   2. Salir\n");
    printf("   3. Autores\n");
    printf("$ ");
    scanf("%d",&opcion);
    return opcion;
  }

  /*
  Función: makeConsult
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: número de líneas del archivos, número de mappers,
  número de reducers.
  Valor de salida: Entero con el resultado de la consulta
  Descripción: Función que hace la creación de los mappers y los reducers y se encarga del control de flujo. Además
  calcula el resultado final de la consulta.
  */
  int makeConsult(int lineas, int n_mappers, int n_reducers)
  {
    int i, val, suma, fin;
    int count_neg, n_neg, status;
    n_neg = n_reducers;
    suma = 0;
    count_neg = 0;
    status = 1;

    for(i = 0; i < n_mappers; i++)
    {
      sem_post(&master_init[i]);
    }

    while(status)
    {
      sem_wait(&n_sem_reducers);
      sem_wait(&r_write);

      val = sumas[0];
      for(i = 0; i < tam_sumas-1; i++)
      {
        sumas[i] = sumas[i+1];
      }
      if(i+1 < BUFFER_SIZE)
      {
        sumas[i] = sumas[i+1];
      }
      tam_sumas--;
      if(val == -1)
      {
        count_neg++;
        if(count_neg == n_neg)
        {
          status = 0;
        }
      }
      else
      {
        suma += val;
      }

      sem_post(&r_write);
      sem_post(&e_sem_reducers);
    }
    return suma;
  }

  /*
  Función: initializeConsult
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: número de lineas del archivos, número de mappers,
  número de reducers
  Valor de salida: Entero con el resultado de la consulta
  Descripción: Función que inicializa la consulta, leyendo los valores, inicializando datos y limpiando las listas
  de la anterior consulta. Imprime el resultado con formato además de retornarlo.
  */
  int initializeConsult(int lineas, int n_mappers, int n_reducers)
  {
    struct timeval start, end;
    char linea[50];
    char* aux_col;
    char* aux_val;
    char* aux;
    int resultado, i,j;
    long calc_time;

    printf("$ ");

    if(criterio!=NULL)
    strcpy(criterio,"");
    columna = valor = -1;

    /*Lectura y tokenizacion de la consulta*/
    scanf("%*c%[^\n]",linea);
    aux = strtok (linea," ,");
    aux_col = aux;

    aux = strtok (NULL," ,");
    criterio = aux;

    aux = strtok (NULL," ,");
    aux_val = aux;

    /*Borrado de los valores globales*/
    if(results != NULL)
    {
      for(i = 0; i < n_reducers; i++)
      {
        if( results != NULL)
        {
          free(results[i]);
        }
        results[i] = NULL;
      }
    }

    if(tams != NULL){
      free(tams);
    }
    if(sumas != NULL){
      free(sumas);
    }
    if(count_neg != NULL){
      free(count_neg);
    }

    tams = NULL;
    sumas = NULL;
    count_neg = NULL;
    results = NULL;

    results = (struct Result**)malloc(sizeof(struct Result*)*n_reducers);
    tams = (int*)malloc(sizeof(int)*n_reducers);
    sumas = (int *)malloc(sizeof(int)*BUFFER_SIZE);
    count_neg = (int *)malloc(sizeof(int)*n_reducers);

    for(i = 0; i < n_reducers; i++)
    {
      results[i] = (struct Result*)malloc(sizeof(struct Result)*BUFFER_SIZE);
      tams[i] = 0;
      sumas[i] = 0;
      count_neg[i] = 0;
    }
    tam_sumas = 0;

    columna = atoi(aux_col);
    valor = atoi(aux_val);

    /*Comprobacion de valores coherentes*/
    if(columna <= 0 || columna > 18)
    {
      printf("Valor de columna invalida\n");
      return(-1);
    }
    if(strcmp(criterio,"=") != 0 && strcmp(criterio,"<") != 0 && strcmp(criterio,">") != 0
    && strcmp(criterio,">=") != 0 && strcmp(criterio,"<=") != 0)
    {
      printf("Valor de criterio invalida\n");
      return(-1);
    }

    gettimeofday(&start, NULL);
    resultado = makeConsult(lineas, n_mappers, n_reducers);
    gettimeofday(&end, NULL);

    calc_time = ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));

    printf("$ El resultado de la consulta fue: %d , con un tiempo de duracion de: %ld microsegundos\n", resultado, calc_time);
    return resultado;
  }

  /*
  Función: killThreads
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: Apuntadores a arreglos con ids de mappers y reducersm, cantidad de mappers y reducers.
  Valor de salida: No tiene
  Descripción: Recorre los arreglos activando la señal que los elimina los hilos correspondientes a los mappers y reducers.
  */
  void killThreads(pthread_t* p_id_map, pthread_t* p_id_red, int n_mappers, int n_reducers)
  {
    int i;

    for(i = 0; i < n_mappers; i++)
    {
      printf("Mapper con ID %d termina\n", i);
      pthread_kill(p_id_map[i],SIGUSR1);
    }
    for(i = 0; i < n_mappers; i++)
    {
      pthread_join(p_id_map[i],NULL);
    }

    for(i = 0; i < n_reducers; i++)
    {
      printf("Reducer con ID %d termina\n", i);
      pthread_kill(p_id_red[i],SIGUSR1);
    }
    for(i = 0; i < n_reducers; i++)
    {
      pthread_join(p_id_red[i],NULL);
    }
  }

  /*
  Función: main
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: número de parámetros de entrada y arreglo con los parámetros de entrada
  Valor de salida: Entero con el resultado de la ejecución del programa
  Descripción: Función principal del programa, la cual llama a la lectura del archivo de Logs, y comprueba
  la integridad de los datos enviados por el usuario.
  */
  int main(int argc, char *argv[])
  {
    if(argc != 5)
    {
      printf("Error. Uso del programa analogp logfile lineas nmappers nreducers\n");
      exit(-1);
    }

    results = NULL;
    tams = NULL;
    sumas = NULL;
    criterio = NULL;
    char line[MAXLIN];
    long n_mappers, n_reducers;
    int lineas;
    long AUX_n_job, AUX_submit_time, AUX_waiting_time, AUX_executing_time, AUX_n_procesors;
    float AUX_cpu_avg_time, AUX_memory_used;
    long AUX_n_asked_procesors, AUX_asked_time;
    long AUX_asked_memory, AUX_status;
    long AUX_id_user, AUX_id_group, AUX_id_application;
    long AUX_n_queue, AUX_n_partition, AUX_last_job, AUX_think_time_last_job;
    pid_t child_pid;
    int opcion, error = 0;
    int inicial,fiinal;
    int lineas_x_mapper, residuo, results_x_reducers;
    FILE *fp;
    int i, j, id_bff, finalResult = 0;
    struct Parameter* parameters;
    struct Parameter* parameters_red;
    pthread_t* thread_pid_m;
    pthread_t* thread_pid_r;

    master_init = NULL;
    tams = NULL;
    m_write = NULL;
    n_sem_mappers = NULL;
    e_sem_mappers = NULL;
    sumas = NULL;
    count_neg = NULL;

    miLOG = (struct Log *)malloc(sizeof(struct Log)*atoi(argv[2]));
    fp = fopen(argv[1], "r");

    i = 0;
    /*Lectura de archivo de Logs*/
    while (mgetline(line, sizeof(line), fp) > 0)
    {
      sscanf(line, "%ld %ld %ld %ld %ld %f %f %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld ",
      &AUX_n_job, &AUX_submit_time, &AUX_waiting_time, &AUX_executing_time, &AUX_n_procesors,
      &AUX_cpu_avg_time, &AUX_memory_used, &AUX_n_asked_procesors, &AUX_asked_time, &AUX_asked_memory, &AUX_status, &AUX_id_user,
      &AUX_id_group, &AUX_id_application, &AUX_n_queue, &AUX_n_partition, &AUX_last_job, &AUX_think_time_last_job );

      asignar(AUX_n_job, AUX_submit_time, AUX_waiting_time, AUX_executing_time, AUX_n_procesors,
        AUX_cpu_avg_time, AUX_memory_used, AUX_n_asked_procesors, AUX_asked_time, AUX_asked_memory, AUX_status, AUX_id_user,
        AUX_id_group, AUX_id_application, AUX_n_queue, AUX_n_partition, AUX_last_job, AUX_think_time_last_job, i++);
      }

      fclose(fp);

      lineas = atoi(argv[2]);
      n_mappers = atoi(argv[3]);
      n_reducers = atoi(argv[4]);

      if(n_reducers > n_mappers)
      {
        printf("El número de reducers es mayor al número de mappers\n");
        error++;
      } if(n_mappers <= 0 ){
        printf("El número de mappers debe ser mayor a cero\n");
        error++;
      } if( n_reducers <= 0 ){
        printf("El número de reducers debe ser mayor a cero\n");
        error++;
      }  if( lineas <= 0 ){
        printf("El número de lineas debe ser mayor a cero\n");
        error++;
      }

      if(error != 0){
        return(-1);
      }

      thread_pid_m = (pthread_t*)malloc(sizeof(pthread_t)*n_mappers);
      thread_pid_r = (pthread_t*)malloc(sizeof(pthread_t)*n_reducers);

      master_init = (sem_t*)malloc(sizeof(sem_t)*n_mappers);
      m_write = (sem_t *)malloc(sizeof(sem_t)*n_reducers);
      n_sem_mappers = (sem_t *)malloc(sizeof(sem_t)*n_reducers);
      e_sem_mappers = (sem_t *)malloc(sizeof(sem_t)*n_reducers);

      for(i = 0; i < n_mappers; i++)
      {
        sem_init(&master_init[i],0,0);
      }

      for(i = 0; i < n_reducers; i++)
      {
        sem_init(&m_write[i],0,1);
        sem_init(&n_sem_mappers[i],0,0);
        sem_init(&e_sem_mappers[i],0,BUFFER_SIZE);
      }
      sem_init(&r_write,0,1);
      sem_init(&n_sem_reducers,0,0);
      sem_init(&e_sem_reducers,0,BUFFER_SIZE);

      signal(SIGUSR1, handler);


      parameters = (struct Parameter*)malloc(sizeof(struct Parameter)*n_mappers);
      inicial = 0;
      lineas_x_mapper = lineas/n_mappers;
      residuo = lineas%n_mappers;

      /*For para la creacion de los parametros de mappers con los intervalos y consultas*/
      for(i = 0; i < n_mappers; i++)
      {
        if(residuo > 0)
        {
          fiinal = inicial + lineas_x_mapper;
        }
        else
        fiinal = inicial + lineas_x_mapper - 1;
        parameters[i].inicial = inicial;
        parameters[i].fiinal = fiinal;
        parameters[i].id_m = i;
        inicial += lineas_x_mapper;
        if(residuo > 0)
        {
          inicial++;
          residuo--;
        }
      }

      /*For para la creacion de los hilos mappers*/

      inicial = 0;
      results_x_reducers = n_mappers/n_reducers;
      residuo = n_mappers%n_reducers;
      parameters_red = (struct Parameter*)malloc(sizeof(struct Parameter)*n_reducers);

      for(i = 0; i < n_reducers; i++)
      {
        if(residuo > 0)
        {
          fiinal = inicial + results_x_reducers;
        }
        else
        fiinal = inicial + results_x_reducers - 1;

        for(j = inicial; j <= fiinal; j++)
        {
          parameters[j].id_r = i;
        }
        inicial += results_x_reducers;
        if(residuo > 0)
        {
          inicial++;
          residuo--;
        }
      }


      for(i = 0; i < n_mappers; i++)
      {
        pthread_create(&thread_pid_m[i], NULL, (void*)mapper, (void*)&parameters[i]);
      }

      inicial = 0;
      results_x_reducers = n_mappers/n_reducers;
      residuo = n_mappers%n_reducers;

      /*For para la creacion de los parametros de reducers con los intervalos y consultas*/
      for(i = 0; i < n_reducers; i++)
      {
        if(residuo > 0)
        {
          fiinal = inicial + results_x_reducers;
        }
        else
        fiinal = inicial + results_x_reducers - 1;

        parameters_red[i].inicial = inicial;
        parameters_red[i].fiinal = fiinal;
        parameters_red[i].id_r = i;
        inicial += results_x_reducers;
        if(residuo > 0)
        {
          inicial++;
          residuo--;
        }
      }

      inicial = 0;
      results_x_reducers = n_mappers/n_reducers;
      residuo = n_mappers%n_reducers;

      for(i = 0; i < n_reducers; i++)
      {
        if(residuo > 0)
        {
          fiinal = inicial + results_x_reducers;
        }
        else
        fiinal = inicial + results_x_reducers - 1;

        parameters_red[i].num_thr = fiinal-inicial+1;

        inicial += results_x_reducers;
        if(residuo > 0)
        {
          inicial++;
          residuo--;
        }
      }

      /*For para la creacion de los hilos reducers*/
      for(i = 0; i < n_reducers; i++)
      {
        pthread_create(&thread_pid_r[i], NULL, (void*)reducer, (void*)&parameters_red[i]);
      }

      do{

        opcion = printMenu();

        switch(opcion)
        {
          case 1:
          finalResult = initializeConsult(lineas, n_mappers, n_reducers);
          break;
          case 2:
          killThreads(thread_pid_m, thread_pid_r, n_mappers, n_reducers);
          break;
          case 3:
          printf("Autores:\n");
          printf("  -Sergio Andres Mejia Tovar\n");
          printf("  -Santiago Palacios Loaiza\n");
          printf("  -Julian David Parada Galvis\n");
          break;
          default:
          printf("Error. Ingrese una opcion correcta.\n");
          break;

        }
      }while(opcion != 2);

      free(miLOG);
      return(0);
    }
