/* archivo: analogp.c
Autores:
Sergio Andrés Mejía Tovar
Santiago Palacios Loaiza
Julian David Parada Galvis
Objetivo: Generar un analizador de logs con formato específico, usando procesos para dividir el archivo
Funciones del programa:

int mgetline (char *line, int max, FILE *f)
void asignar(struct Log *miLOG, long AUX_n_job,	..., long AUX_think_time_last_job, int pos)
int compare(int num, char* criterio, int valor)
void floatToChar(float num, char* res)
void saveMapperResults(struct Result* results, int tam, int id)
void saveReducerResults(int valor, int id)
void reducer(int inicial, int final, int id, int intermedios)
int sumResults(int tam, int intermedios)
void imprimir(struct Log *logs, int inicial, int final)
int printMenu()
int makeConsult(struct Log* miLOG, int lineas, int n_mappers, int n_reducers, int columna, char* criterio, int valor, int intermedios)
void eraseFiles()
int initializeConsult(struct Log* miLOG, int lineas, int n_mappers, int n_reducers, int intermedios)
int main(int argc, char *argv[])

Fecha de finalización: 18 de Marzo de 2019

*/

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <wait.h>
#include <time.h>
#include <sys/time.h>
#include "log.h"
#include "result.h"

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
Parámetros de Entrada: apuntador a un arreglo de struct Log. Valores a guardar en el arreglo en la posición dada en pos.
Valor de salida: No tiene
Descripción: Recibe un arreglo de Logs y una posición. Guarda los datos dados en el
arreglo en la posición dada en los datos correspondientes.
*/
void asignar(struct Log *miLOG, long AUX_n_job,	long AUX_submit_time,	long AUX_waiting_time,
  long AUX_executing_time,	long AUX_n_procesors,	float AUX_cpu_avg_time,	float AUX_memory_used,
  long AUX_n_asked_procesors,	long AUX_asked_time,	long AUX_asked_memory,	long AUX_status,	long AUX_id_user,
  long AUX_id_group,	long AUX_id_application,	long AUX_n_queue,	long AUX_n_partition,
  long AUX_last_job,	long AUX_think_time_last_job, int pos)
  {
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
    if(strcmp(criterio,">") == 0)
    {
      if(num > valor)
      {
        return 1;
      }
    }
    else if(strcmp(criterio,"<") == 0)
    {
      if(num < valor)
      {
        return 1;
      }
    }
    else if(strcmp(criterio,"<=") == 0)
    {
      if(num <= valor)
      {
        return 1;
      }
    }
    else if(strcmp(criterio,">=") == 0)
    {
      if(num >= valor)
      {
        return 1;
      }
    }
    else if(strcmp(criterio,"=") == 0)
    {
      if(num == valor)
      {
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

    res = malloc(sizeof(myFloat));
    ret = snprintf(res, sizeof(res)*2, "%f\n", myFloat);

    if(ret < 0)
    {
      perror("snprintf: ");
      exit(1);
    }
  }

  /*
  Función: saveMapperResults
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a una arreglo de struct Result, tamaño del arreglo y id del mapper
  Valor de salida: No tiene
  Descripción: Guarda el arreglo de resultados (de la forma K, V) del mapper en el archivo correspondiente
  */
  void saveMapperResults(struct Result* results, int tam, int id)
  {
    FILE *fp;
    char file_name[30];
    char message[40];
    char string_id[10];
    char resultados[10];
    int i, revision;

    /*Se va creando el nombre del archivo de cada mapper*/
    strcpy(file_name,"map");
    revision = snprintf(string_id,sizeof(string_id),"%d",id);
    if(revision < 0)
    {
      perror("snprintf: ");
      exit(1);
    }
    strcat(file_name,string_id);
    fp = fopen(file_name, "w+");
    for(i = 0; i < tam; i++)
    {
      revision = snprintf(resultados,sizeof(resultados),"%d",results[i].fila);
      if(revision < 0)
      {
        perror("snprintf: ");
        exit(1);
      }
      /*Creacion del par K,V para enviar al archivo*/
      strcpy(message,resultados);
      strcat(message, " ");
      strcat(message, results[i].result);
      strcat(message,"\n");

      fputs( message, fp );

      strcpy(message,"");
    }/*Cierre for que escribe el archivo de mapper*/
    fclose(fp);
  }

  /*
  Función: saveReducerResults
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: Entero con el valor del análisis del reducer, id del reducer
  Valor de salida: No tiene
  Descripción: Guarda el valor hallado por el reducer en el archivo de reducer correspondiente
  */
  void saveReducerResults(int valor, int id)
  {
    FILE *fp;
    int i, ret;
    char val[10];
    char string_id[10];
    char file_name[15];
    /*Se va creando el nombre del archivo de cada reducer*/
    strcpy(file_name,"red");
    ret = snprintf(string_id,sizeof(string_id),"%d",id);
    if(ret < 0)
    {
      perror("snprintf: ");
      exit(1);
    }
    strcat(file_name,string_id);
    fp = fopen(file_name, "w+");
    ret = snprintf(val, sizeof(val), "%d", valor);
    if(ret < 0)
    {
      perror("snprintf: ");
      exit(1);
    }
    fputs(val, fp );
    fclose(fp);
  }

  /*
  Función: mapper
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a un arreglo de struct Log, id del mapper.
  Intervalo [inicial, final] de los logs a analizar. Columna, criterio y valor de la consulta a realizar.
  Valor de salida: No tiene
  Descripción: Recorre el arreglo de Logs en el intervalo [inicial, final], buscando aquellos que corresponden
  con la consulta (utilizando la función compare). Luego llama a la función saveMapperResults para guardar sus resultados.
  */
  void mapper(struct Log* array, int id, int inicial, int final, int columna, char* criterio, int valor)
  {
    int i,j, ret;
    int res_comp;
    struct Result *results;
    struct Result* aux_results;
    char datos[30];
    int tam;
    tam = 0;
    results = NULL;
    for(i = inicial; i <= final; i++)
    {
      if(columna == 6 || columna == 7)
      {
        res_comp = compare(array[i].farray[columna-6],criterio,valor);
        if(res_comp == 1)
        {
          /*Creacion de un nuevo arreglo dinamico con los datos del anterior y el nuevo dato*/
          aux_results = (Result *)malloc(sizeof(struct Result)*(tam+1));
          for(j = 0; j < tam; j++)
            aux_results[j] = results[j];
          aux_results[tam].fila = i;
          aux_results[tam].result = (char *)malloc(sizeof(datos)*strlen(datos));
          floatToChar(array[i].farray[columna-6], aux_results[tam].result);
          tam++;
          if(results != NULL)
            free(results);
          results = aux_results;
          aux_results = NULL;
        }/*Cierre if de res_comp == 1*/
      }/*Cierre if de columnas == 6 o 7*/
      else
      {
        res_comp = compare(array[i].larray[columna-1],criterio,valor);
        if(res_comp == 1)
        {
          /*Creacion de un nuevo arreglo dinamico con los datos del anterior y el nuevo dato*/
          aux_results = (Result *)malloc(sizeof(struct Result)*(tam+1));
          for(j = 0; j < tam; j++)
          aux_results[j] = results[j];
          aux_results[tam].fila = i+1;
          ret = snprintf(datos, sizeof(datos),"%ld",array[i].larray[columna-1]);
          if(ret < 0)
          {
            perror("snprintf: ");
            return;
          }
          aux_results[tam].result = (char *)malloc(sizeof(datos)*strlen(datos));
          strcpy(aux_results[tam].result,datos);
          tam++;
          if(results != NULL)
          free(results);
          results = aux_results;
          aux_results = NULL;
        }/*Cierre if de res_comp == 1*/
      }/*Ciere else para comparar las demas columnas*/
    }/*Cierre for para buscar en los logs correspondientes*/
    saveMapperResults(results, tam, id);
  }

  /*
  Función: reducer
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: Intervalor [inicial,final] correspondiente a los mappers que debe analizar. Id del reducer
  e indicador intermedios
  Valor de salida: No tiene
  Descripción: Abre los archivos escritos por los mappers y suma el número de ocurrencias. Si intermedios==0, los archivos
  se borran y luego escribe su resultado en un archivo llamando a saveReducerResults.
  */
  void reducer(int inicial, int final, int id, int intermedios)
  {
    int i, revision;
    int valor;
    FILE *fp;
    char file_name[30];
    char num_map[10];
    char line[20];
    valor = 0;
    for(i = inicial; i <= final; i++)
    {
      /*Se va creando el nombre los archivos de los mappers a leer*/
      strcpy(file_name,"map");
      revision = snprintf(num_map,sizeof(num_map),"%d",i);
      if(revision < 0)
      {
        perror("snprintf: ");
        exit(1);
      }
      strcat(file_name,num_map);
      fp = fopen(file_name, "r");
      while (mgetline(line, sizeof(line), fp) > 0)
      {
        valor++;
      }
      fclose(fp);
      if(intermedios == 0){
        remove(file_name);
      }
    }/*Cierre for que lee los archivos de los mappers y crea los reducers*/
    saveReducerResults(valor, id);
  }

  /*
  Función: sumResults
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: cantidad de reducers y indicador de intermedios
  Valor de salida: Entero con la suma de los valores de los archivos
  Descripción: Lee los archivos generados por los reducers y suma estos valores para
  obtener el resultado final de la operación.
  */
  int sumResults(int tam, int intermedios)
  {
    int i, revision;
    int valor, suma_total;
    FILE *fp;
    char file_name[30];
    char num_map[10];
    char line[20];

    suma_total = 0;
    for(i = 0; i < tam; i++)
    {
      /*Se va creando el nombre del archivo de acuerdo al numero de reducers*/
      strcpy(file_name,"red");
      revision = snprintf(num_map,sizeof(num_map),"%d",i);
      if(revision < 0)
      {
        perror("snprintf: ");
        exit(1);
      }
      strcat(file_name,num_map);
      fp = fopen(file_name, "r");
      /*Se lee el numero de lineas del archivo*/
      while (mgetline(line, sizeof(line), fp) > 0)
      {
        sscanf(line, "%d", &valor);
        suma_total += valor;
      }
      fclose(fp);
      if(intermedios == 0){
        remove(file_name);
      }
    }/*Cierre for que lee los archivos que salen de los reducers*/
    return(suma_total);
  }

  /*
  Función: imprimir
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a arreglo de struct Log, Intervalo [inicial,final] del arreglo
  Valor de salida: No tiene
  Descripción: Imprime dos valores del log en el intervalo dado. Funcion auxiliar usada para
  comprobar lectura de archivos.
  */
  void imprimir(struct Log *logs, int inicial, int final)
  {
    int i;

    printf("Logs:\n");

    for(i=inicial; i <= final; i++)
    {
      printf("%ld %ld\n", logs[i].larray[0], logs[i].larray[4]);
    }
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
  Parámetros de Entrada: apuntador a un arreglo de struct Log, número de líneas del archivos, número de mappers,
  número de reducers, valores de la consulta (columna, criterio y valor) e indicador intermedios
  Valor de salida: Entero con el resultado de la consulta
  Descripción: Función que hace la creación de los mappers y los reducers y se encarga del control de flujo. Además
  calcula el resultado final de la consulta.
  */
  int makeConsult(struct Log* miLOG, int lineas, int n_mappers, int n_reducers, int columna, char* criterio, int valor, int intermedios)
  {
    int inicial,final, fin;
    int lineas_x_mapper, residuo, mappers_x_reducers;
    pid_t child_pid;
    int i;
    int status;

    inicial = 0;
    lineas_x_mapper = lineas/n_mappers;
    residuo = lineas%n_mappers;
    /*Creacion de los mappers, teniendo en cuenta los intervalos creados*/
    for(i = 0; i < n_mappers; i++)
    {
      if(residuo > 0)
      {
        final = inicial + lineas_x_mapper;
      }
      else
      final = inicial + lineas_x_mapper - 1;

      if((child_pid = fork()) < 0)
      {
        perror("fork:");
        exit(1);
      }
      if(child_pid == 0)
      {
        mapper(miLOG, i, inicial, final, columna, criterio, valor);
        exit(0);
      }
      inicial += lineas_x_mapper;
      if(residuo > 0)
      {
        inicial++;
        residuo--;
      }
    }/*Cierre for que crea los mappers*/

    for(i = 0; i < n_mappers; i++)
    wait(&status);

    inicial = 0;
    mappers_x_reducers = n_mappers/n_reducers;
    residuo = n_mappers%n_reducers;

    /*Creacion de los reducers, teniendo en cuenta los intervalos creados*/
    for(i = 0; i < n_reducers; i++)
    {
      if(residuo > 0)
      {
        final = inicial + mappers_x_reducers;
      }
      else
      final = inicial + mappers_x_reducers - 1;
      if((child_pid = fork()) < 0)
      {
        perror("fork:");
        exit(1);
      }
      if(child_pid == 0)
      {
        reducer(inicial, final, i, intermedios);
        exit(0);
      }
      inicial += mappers_x_reducers;
      if(residuo > 0)
      {
        inicial++;
        residuo--;
      }
    }/*Cierre for que crea los reducers*/

    for(i = 0; i < n_reducers; i++)
    wait(&status);

    fin = sumResults(n_reducers, intermedios);
    return fin;
  }

  /*
  Función: eraseFiles
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: No tiene
  Valor de salida: No tiene
  Descripción: Borra todos los archivos de mappers y reducers que puedan existir en el directorio actual.
  */
  void eraseFiles(){
    DIR * dir;
    struct dirent *entrada;
    dir = opendir(".");
    char *aux;
    char buf[30];
    if (dir == NULL)
    {
      return;
    }
    while ((entrada = readdir(dir)) != NULL)
    {
      if (strstr(entrada->d_name,"map")!=NULL || strstr(entrada->d_name,"red")!=NULL)
      {
        remove(entrada->d_name);
      }
    }/*Cierre while*/
  }

  /*
  Función: initializeConsult
  Autores: S. Mejia, S. Palacios, J. Parada
  Parámetros de Entrada: apuntador a arreglo de struct Log, número de lineas del archivos, número de mappers,
  número de reducers e identificador intermedios
  Valor de salida: Entero con el resultado de la consulta
  Descripción: Función que inicializa la consulta, leyendo los valores, inicializando datos y borrando los archivos
  de la anterior consulta. Imprime el resultado con formato además de retornarlo.
  */
  int initializeConsult(struct Log* miLOG, int lineas, int n_mappers, int n_reducers, int intermedios)
  {
    struct timeval start, end;
    char linea[50];
    char* criterio;
    char* aux_col;
    char* aux_val;
    char* aux;
    int columna, valor, resultado;
    long calc_time;

    printf("$ ");

    /*Lectura y tokenizacion de la consulta*/
    scanf("%*c%[^\n]",linea);
    aux = strtok (linea,", ");
    aux_col = aux;

    aux = strtok (NULL,", ");
    criterio = aux;

    aux = strtok (NULL,", ");
    aux_val = aux;

    columna = atoi(aux_col);
    valor = atoi(aux_val);

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

    eraseFiles();

    gettimeofday(&start, NULL);
    resultado = makeConsult(miLOG, lineas, n_mappers, n_reducers, columna, criterio, valor, intermedios);
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
    if(argc != 6)
    {
      printf("Error. Uso del programa analogp logfile lineas nmappers nreducers intermedios\n");
      exit(-1);
    }

    struct Log *miLOG;
    char line[MAXLIN];
    long n_mappers, n_reducers;
    int intermedios, lineas;
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

    /*Lectura de las lineas de los archivos y guardado en el arreglo de Logs*/
    while (mgetline(line, sizeof(line), fp) > 0)
    {
      sscanf(line, "%ld %ld %ld %ld %ld %f %f %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld ",
      &AUX_n_job, &AUX_submit_time, &AUX_waiting_time, &AUX_executing_time, &AUX_n_procesors,
      &AUX_cpu_avg_time, &AUX_memory_used, &AUX_n_asked_procesors, &AUX_asked_time, &AUX_asked_memory, &AUX_status, &AUX_id_user,
      &AUX_id_group, &AUX_id_application, &AUX_n_queue, &AUX_n_partition, &AUX_last_job, &AUX_think_time_last_job );

      asignar(miLOG, AUX_n_job, AUX_submit_time, AUX_waiting_time, AUX_executing_time, AUX_n_procesors,
        AUX_cpu_avg_time, AUX_memory_used, AUX_n_asked_procesors, AUX_asked_time, AUX_asked_memory, AUX_status, AUX_id_user,
        AUX_id_group, AUX_id_application, AUX_n_queue, AUX_n_partition, AUX_last_job, AUX_think_time_last_job, i++);
      }/*Cierre while de lectura de archivo*/

      fclose(fp);

      lineas = atoi(argv[2]);
      n_mappers = atoi(argv[3]);
      n_reducers = atoi(argv[4]);
      intermedios = atoi(argv[5]);

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
      } if(intermedios != 0 && intermedios != 1){
        printf("La opción de intermedios debe ser solo binaria\n");
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
          finalResult = initializeConsult(miLOG, lineas, n_mappers, n_reducers, intermedios);
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
