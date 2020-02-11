
#define MAXLOG 10000
#define MAXLIN 1000
#define BUFFER_SIZE 500

typedef struct Log
{
	/*long n_job;//1
	long submit_time;//2
	long waiting_time;//3
	long executing_time;//4
	long n_procesors;//5
	float cpu_avg_time;//6
	float memory_used;//7
	long n_asked_procesors;//8
	long asked_time;//9
	long asked_memory;//10
	long status;//11
	long id_user;//12
	long id_group;//13
	long id_application;//14
	long n_queue;//15
	long n_partition;//16
	long last_job;//17
	long think_time_last_job;//18*/
	long larray[18];
	float farray[2];
} Log;
