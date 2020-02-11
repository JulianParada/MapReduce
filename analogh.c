/* archivo: analogp.c
Autores:
Sergio Andrés Mejía Tovar
Santiago Palacios Loaiza
Julian David Parada Galvis
Objetivo: Generar un analizador de logs con formato específico, usando hilos para dividir el archivo
Funciones del programa:

int mgetline (char *line, int max, FILE *f)
void asignar(struct Log *miLOG, long AUX_n_job,	..., long AUX_think_time_last_job, int pos)
int compare(int num, char* criterio, int valor)
void floatToChar(float num, char* res)
void* mapper(struct Parameter* param)
void* reducer(struct Parameter *param)
nt sumResults(int n_reducers)
void imprimir(int inicial, int final)
int printMenu()
int makeConsult(int lineas, int n_mappers, int n_reducers, int columna, char* criterio, int valor)
int initializeConsult(int lineas, int n_mappers, int n_reducers)
int main(int argc, char *argv[])

Fecha de finalización: 18 de Marzo de 2019

*/

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <pthread.h>
#include <wait.h>
#include <time.h>
#include <sys/time.h>
#include "log.h"
#include "parameters.h"
#include "result.h"

struct Log *miLOG;
struct Result **results;
int *tams;
int *sumas;

/*
Función: mgetline
Autores: Mariela Curiel
Parámetros de Entrada: Cadena de caracteres que guardará la lectura de una
línea de un archivo dado en FILE *f y su tamaño máximo max
Valor de salida: Tamaño de la línea leída
Descripción: Lee la siguiente lińea de un archivo dado y lo guarda en la cadena de caracteres dad
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
  Parámetros de Entrada: Número a evaluar, el criterio dado
  (cadena con posibles valores ">","<",">=","<=","=") y el valor a comparar
  Valor de salida: Entero con la respuesta, siendo 1 true y 0 false
  Descripción: Recibe un número y lo compara con el valor dado teniendo en cuenta el criterio enviado.
  */
  int compare(int num, char* criterio, int valor)
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
  */
  void* mapper(struct Parameter* param)
  {
    //printf("Hilo %d",param->id);
    int i, j, ret;
    int res_comp;
    char datos[30];
    struct Result* aux_results;
    for(i = param->inicial; i <= param->final; i++)
    {
      if(param->columna == 6 || param->columna == 7)
      {
        res_comp = compare(miLOG[i].farray[param->columna - 6],param->criterio,param->valor);
        if(res_comp == 1)
        {
          /*Creacion de un nuevo arrego con los datos anteriores y el nuevo*/
          aux_results = (Result *)malloc(sizeof(struct Result)*(tams[param->id]+1));
          for(j = 0; j < tams[param->id]; j++)
            aux_results[j] = results[param->id][j];
          aux_results[tams[param->id]].fila = i;
          aux_results[tams[param->id]].result = (char *)malloc(sizeof(datos)*strlen(datos));
          floatToChar(miLOG[i].farray[param->columna-6], aux_results[tams[param->id]].result);
          tams[param->id]++;
          if(results[param->id] != NULL)
          free(results[param->id]);
          results[param->id] = aux_results;
          aux_results = NULL;
        }
      }
      else
      {
        res_comp = compare(miLOG[i].larray[param->columna-1],param->criterio,param->valor);

        if(res_comp == 1)
        {
          /*Creacion de un nuevo arrego con los datos anteriores y el nuevo*/
          aux_results = (Result *)malloc(sizeof(struct Result)*(tams[param->id]+1));
          for(j = 0; j < tams[param->id]; j++){
            aux_results[j] = results[param->id][j];
          }
          aux_results[tams[param->id]].fila = i+1;
          ret = snprintf(datos, sizeof(datos),"%ld",miLOG[i].larray[param->columna-1]);
          if(ret < 0)
          {
            perror("snprintf: ");
            pthread_exit(NULL);
          }

          aux_results[tams[param->id]].result = (char *)malloc(sizeof(datos)*strlen(datos));
          strcpy(aux_results[tams[param->id]].result,datos);
          if(results[param->id] != NULL)
          free(results[param->id]);
          results[param->id] = aux_results;
          tams[param->id]++;
          aux_results = NULL;
        }
      }
    }


    pthread_exit(NULL);
  }

  /*
  Función: reducer
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a una estructura de los parametros de un hilo/mapper.
  Valor de salida: No tiene
  Descripción: Recorre el arreglo de resultados global en las posiciones que le corresponden contando los tamaños de los resultados del mapper.
  */
  void* reducer(struct Parameter *param)
  {
    int i, revision;
    int valor;
    valor = 0;

    for(i = param->inicial; i <= param->final; i++)
    {
      valor += tams[i];
    }
    sumas[param->id] = valor;
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
  void imprimir(int inicial, int final)
  {
    int i;

    printf("Logs:\n");

    for(i=inicial; i <= final; i++)
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
  número de reducers, valores de la consulta (columna, criterio y valor).
  Valor de salida: Entero con el resultado de la consulta
  Descripción: Función que hace la creación de los mappers y los reducers y se encarga del control de flujo. Además
  calcula el resultado final de la consulta.
  */
  int makeConsult(int lineas, int n_mappers, int n_reducers, int columna, char* criterio, int valor)
  {
    int inicial,final, fin;
    int lineas_x_mapper, residuo, results_x_reducers;
    pthread_t* thread_pid_m;
    pthread_t thread_pid_r;
    int i, val, suma;
    int status;
    struct Parameter* parameters;

    thread_pid_m = (pthread_t*)malloc(sizeof(pthread_t)*n_mappers);
    parameters = (struct Parameter*)malloc(sizeof(struct Parameter)*n_mappers);
    inicial = 0;
    lineas_x_mapper = lineas/n_mappers;
    residuo = lineas%n_mappers;
    suma = 0;

    /*For para la creacion de los parametros de mappers con los intervalos y consultas*/
    for(i = 0; i < n_mappers; i++)
    {
      if(residuo > 0)
      {
        final = inicial + lineas_x_mapper;
      }
      else
      final = inicial + lineas_x_mapper - 1;
      parameters[i].inicial = inicial;
      parameters[i].final = final;
      parameters[i].id = i;
      parameters[i].columna = columna;
      parameters[i].criterio = (char *)malloc(sizeof(char)*strlen(criterio));
      strcpy(parameters[i].criterio,criterio);
      parameters[i].valor = valor;
      inicial += lineas_x_mapper;
      if(residuo > 0)
      {
        inicial++;
        residuo--;
      }
    }

    /*For para la creacion de los hilos mappers*/
    for(i = 0; i < n_mappers; i++)
    {
      pthread_create(&thread_pid_m[i], NULL, (void*)mapper, (void*)&parameters[i]);
      //pthread_join(thread_pid,NULL);
    }
    free(parameters);

    for(i = 0; i < n_mappers; i++)
    {
        pthread_join(thread_pid_m[i],NULL);
    }

    inicial = 0;
    results_x_reducers = n_mappers/n_reducers;
    residuo = n_mappers%n_reducers;
    parameters = (struct Parameter*)malloc(sizeof(struct Parameter)*n_reducers);

    /*For para la creacion de los parametros de reducers con los intervalos y consultas*/
    for(i = 0; i < n_reducers; i++)
    {
      if(residuo > 0)
      {
        final = inicial + results_x_reducers;
      }
      else
      final = inicial + results_x_reducers - 1;

      parameters[i].inicial = inicial;
      parameters[i].final = final;
      parameters[i].id = i;

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
      pthread_create(&thread_pid_r, NULL, (void*)reducer, (void*)&parameters[i]);
    }

    for(i = 0; i < n_reducers; i++)
      pthread_join(thread_pid_r,NULL);

    fin = sumResults(n_reducers);
    return fin;
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
    char* criterio;
    char* aux_col;
    char* aux_val;
    char* aux;
    int columna, valor, resultado, i;
    long calc_time;

    printf("$ ");

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
      for(i = 0; i < n_mappers; i++)
      {
        if( results != NULL)
        {
          free(results[i]);
        }
      }
    }

    if(tams != NULL){
      free(tams);
    }
    if(sumas != NULL){
      free(sumas);
    }

    results = (Result**)malloc(sizeof(Result*)*n_mappers);
    tams = (int*)malloc(sizeof(int)*n_mappers);
    sumas = (int *)malloc(sizeof(int)*n_reducers);
    for(i = 0; i < n_mappers; i++)
    {
      results[i] = NULL;
      tams[i] = 0;
    }

    for(i = 0; i < n_reducers; i++){
      sumas[i] = 0;
    }

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
    resultado = makeConsult(lineas, n_mappers, n_reducers, columna, criterio, valor);
    gettimeofday(&end, NULL);

    calc_time = ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));

    printf("$ El resultado de la consulta fue: %d , con un tiempo de duracion de: %ld microsegundos\n", resultado, calc_time);
    return resultado;
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
    FILE *fp;
    int i, finalResult = 0;

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
      do{

        opcion = printMenu();

        switch(opcion)
        {
          case 1:
          finalResult = initializeConsult(lineas, n_mappers, n_reducers);
          break;
          case 2:
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
