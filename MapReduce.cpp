/* ---------------------------------------------------------------
Práctica 3.
Código fuente: MapReduce.cpp
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#include "MapReduce.h"
#include "Types.h"
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stack>
#include "MyQueue.h"
using namespace std;

int splitBigFiles(char *input);
int getNumberFiles(char *input);
int getNamesFiles(char *names[], char *input);
void Fases_Concurentes_1(struct thread_data_1 *data_1);
void Fases_Concurentes_2(struct thread_data_2 *data_2);

struct thread_data_2
{
	int numReducer;
	MapReduce *myObject;
};

struct statistics_split
{
	int numFilesReaded;
	int bytesReaded;
	int numLinesReaded;
	int numTuplesGenerated;
};

struct statistics_map
{
	int numInputTuples;
	int bytesProcessed;
	int numOutputTuples;
};

struct statistics_suffle
{
	int numOutputTuples;
	int numKeysGenerated;
};

struct statistics_reduce
{
	int numKeysProcesed;
	int numOcurences;
	int numBytesWrite;
};

struct thread_data_1
{
	char input_folder[256];
	char input_path[256];
	// int start_line, end_line;
	MapReduce *myObject;
	PtrMap map;
	// struct statistics_split *est_split;
	// struct statistics_map *est_map;
};

int numficheros = 0;
int numBytes = 0;

int numReducers = 0;
int nreducers = 0;

// Split Statistics (Global)
int TotalSplit_archivos = 0;
int TotalSplit_bytesReaded = 0;
int TotalSplit_numLinesReaded = 0;
int TotalSplit_numTuples = 0;

// contador test
int count_suffle = 0;
int count_reduce = 0;

// Map Statistics (Global)
int TotalMap_numInputTuples = 0;
int TotalMap_bytesProcessed = 0;
int TotalMap_numOutputTuples = 0;

// Prints Globals
bool printSplit = true;
bool printMap = true;
bool printSuffle = true;
bool printReduce = true;

int TotalReduce_NumKeys = 0;
int TotalReduce_NumOcur = 0;
int TotalReduce_AvgOcur = 0;
int TotalReduce_NumOutBytes = 0;

pthread_barrier_t barrierSplit;
pthread_barrier_t barrierMap;
pthread_barrier_t EndMap;
pthread_barrier_t EndSuffle;
pthread_barrier_t EndReducer;

pthread_mutex_t mutex1;
pthread_mutex_t mutexFicheros;
pthread_mutex_t mutexStatistics;
pthread_mutex_t Suffle_part;
pthread_mutex_t Reduce_part;
// MyQueue <queue> = new MyQueue<int>[5];
//  sufflequeue = new MyQueue<int>[Reducers.size()];

// Constructor MapReduce: directorio/fichero entrada, directorio salida, función Map, función reduce y número de reducers a utilizar.
MapReduce::MapReduce(char *input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers)
{
	MapFunction = mapf;
	ReduceFunction = reducef;
	InputPath = input;
	OutputPath = output;

	if (debug)
		printf("nreducers: %d\n", nreducers);

	for (int x = 0; x < nreducers; x++)
	{
		char filename[256];
		sprintf(filename, "%s/result.r%d", OutputPath, x + 1);
		AddReduce(new TReduce(ReduceFunction, filename));
	}
}

// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
TError
MapReduce::Run(int nreducers2)
{
	int nfiles = getNumberFiles(InputPath); // numero de los ficheros para crear los arrays
	nreducers = nreducers2;
	// creacion de los threads array
	pthread_t *thread_ids_1;
	thread_ids_1 = (pthread_t *)malloc(sizeof(pthread_t[nfiles]));

	pthread_t *thread_ids_2;
	thread_ids_2 = (pthread_t *)malloc(sizeof(pthread_t[nreducers]));

	// estructuras de datos para los threads
	struct thread_data_1 *data_1;
	data_1 = (thread_data_1 *)malloc(sizeof(thread_data_1[nfiles]));
	if (data_1 == NULL)
		error("malloc1 error\n");

	// estructuras de datos para los segundos threads // Crear tantos como se especifiquen por parametro
	struct thread_data_2 *data_2;
	data_2 = (thread_data_2 *)malloc(sizeof(thread_data_2[nreducers]));
	if (data_2 == NULL)
		error("malloc2 error\n");

	char *names[nfiles];			 // nombre de los ficheros
	getNamesFiles(names, InputPath); // función para leer los ficheros

	// Rellenar la structura con el inputPath y el MAP
	for (int i = 0; i < nreducers; i++)
	{
		data_2[i].numReducer = i;
		data_2[i].myObject = this;
	}
	for (int i = 0; i < nfiles; i++)
	{
		strcpy(data_1[i].input_path, names[i]);
	}
	for (int i = 0; i < nfiles; i++)
	{
		data_1[i].map = new TMap(MapFunction);
		strcpy(data_1[i].input_folder, InputPath);
		data_1[i].myObject = this;
		// data_1[i].est_split= (statistics_split *)malloc(sizeof(statistics_split[1]));
		// data_1[i].est_map = (statistics_map *)malloc(sizeof(statistics_map[1]));
		printf("Input path %s: \n", data_1[i].input_path);
	}
	TotalSplit_archivos = nfiles;
	numficheros = nfiles;
	pthread_barrier_init(&barrierSplit, NULL, nfiles); ///////////////////////////////////////////////////////
	pthread_barrier_init(&barrierMap, NULL, nfiles);   ///////////////////////////////////////////////////////
	pthread_barrier_init(&EndMap, NULL, nfiles);
	pthread_barrier_init(&EndSuffle, NULL, nreducers);
	pthread_barrier_init(&EndReducer, NULL, nreducers);
	// Primeros threads
	printf("\n\x1B[32mProcesando Fase 1 con %d threads...\033[0m\n", nfiles);
	for (int i = 0; i < nfiles; i++)
	{
		if (debug)
			printf("> Thread %d creado Fase 1 \n", i);

		int s = pthread_create(&thread_ids_1[i], NULL, (void *(*)(void *))Fases_Concurentes_1, (void *)&data_1[i]);
		if (s != 0)
		{
			// error("pthread_create");
			error("Run::Concurent 1 - Error Create Thread");
		}
	}
	for (int i = 0; i < nfiles; i++)
	{
		if (debug)
			printf("> Thread %d join Fase 1\n", i);
		pthread_join(thread_ids_1[i], (void **)NULL);
	}

	printf("NumTotalBytes: %i", numBytes);

	// // Segundos threads
	// printf("\n\x1B[32mProcesando Fase 2 con %d threads...\033[0m\n", nreducers);
	// for (int i = 0; i < nreducers; i++)
	// {
	// 	if (debug)
	// 		printf("> Thread %d creado Fase 2 \n", i);

	// 	int s = pthread_create(&thread_ids_2[i], NULL, (void *(*)(void *))Fases_Concurentes_2, (void *)&data_2[i]);
	// 	if (s != 0)
	// 	{
	// 		error("Run::Concurent 2 - Error Create Thread");
	// 	}
	// }
	// for (int i = 0; i < nreducers; i++)
	// {
	// 	if (debug)
	// 		printf("> Thread %d join Fase 2\n", i);
	// 	pthread_join(thread_ids_2[i], (void **)NULL);
	// }

	return (COk);
}

void Fases_Concurentes_1(struct thread_data_1 *data_1)
{
	char *full_path = data_1->input_folder;

	if (debug)
		printf("Input path: %s\n", full_path);
	strcat(full_path, "/");
	strcat(full_path, data_1->input_path);
	if (debug)
		printf("Full path:  %s\n", full_path);

	// **** Split *****
	if (data_1->myObject->Split(full_path, data_1) != COk)
		error("MapReduce::Concurent 1 - Error Split");
	// Split Statistics (TODO: Poner en una funcion a parte)
	printf("Split -> Thread:%ld  ConArchivo:%s   \tbytesReaded:%i  \tnumLinesReaded:%i  \tnumTuples:%i  \n",
		   pthread_self(), data_1->input_path, data_1->map->GetSplit_bytesReaded(), data_1->map->GetSplit_numLinesReaded(), data_1->map->GetSplit_numTuples());

	pthread_mutex_lock(&mutexStatistics);
	// numBytes = numBytes + data_1->est_split->bytesReaded;
	TotalSplit_bytesReaded += data_1->map->GetSplit_bytesReaded();
	TotalSplit_numLinesReaded += data_1->map->GetSplit_numLinesReaded();
	TotalSplit_numTuples += data_1->map->GetSplit_numTuples();
	pthread_mutex_unlock(&mutexStatistics);

	pthread_barrier_wait(&barrierSplit); // Final de la fase de Split
	if (printSplit == true)
	{
		printSplit = false;
		printf("\x1B[33m*** Total Split  -> NTotalArchvios:%i   \tbytesReaded:%i  \tnumLinesReaded:%i  \tnumTuples:%i  \033[0m\n",
			   TotalSplit_archivos, TotalSplit_bytesReaded, TotalSplit_numLinesReaded, TotalSplit_numTuples);
	}

	// **** Map *****
	if (data_1->myObject->Map(data_1) != COk)
		error("MapReduce::Concurent 1 - Error Map");
	// Map Statistics (TODO: Poner en una funcion a parte)
	printf("Map  ->  Thread:%ld  ConArchivo:%s   \tnumInputTuples:%i  \tbytesProcessed:%i  \tnumOutputTuples:%i\n",
		   pthread_self(), data_1->input_path, data_1->map->GetMap_numInputTuples(), data_1->map->GetMap_bytesProcessed(), data_1->map->GetMap_numOutputTuples());

	pthread_mutex_lock(&mutexStatistics);
	TotalMap_numInputTuples += data_1->map->GetMap_numInputTuples();
	TotalMap_bytesProcessed += data_1->map->GetMap_bytesProcessed();
	TotalMap_numOutputTuples += data_1->map->GetMap_numOutputTuples();
	pthread_mutex_unlock(&mutexStatistics);

	pthread_barrier_wait(&barrierMap); // Final de la fase de Split
	if (printMap == true)
	{
		printMap = false;
		printf("\x1B[33m*** Total Map  -> NTotalArchvios:%i   \tnumInputTuples:%i  \tbytesProcessed:%i  \tnumOutputTuples:%i  \033[0m\n",
			   TotalSplit_archivos, TotalMap_numInputTuples, TotalMap_bytesProcessed, TotalMap_numOutputTuples);
	}

	pthread_mutex_lock(&mutexStatistics);
	data_1->myObject->AddMap(data_1->map);
	data_1->myObject->AddStackMap(data_1->map);
	// printf("Añadiendo mapa al stack: \n");
	pthread_mutex_unlock(&mutexStatistics);

	pthread_barrier_wait(&EndMap);
	// pthread_mutex_lock(&mutexStatistics);

	// Reducir numero de threads de 21 a parametro
	// pthread_mutex_lock(&mutexStatistics);
	if (numReducers < nreducers)
	{
		numReducers += 1;
		// printf("\x1B[33m %ld Solo threads Suffle Reducer: %i \033[0m\n", pthread_self(), numReducers);

		// **** Suffle *****
		//data_1->myObject->Suffle(data_1);
		if (data_1->myObject->Suffle(data_1) != COk)
			error("MapReduce::Concurent 1 - Error Shuffle");
		pthread_barrier_wait(&EndSuffle);
		if (printSuffle == true)
		{
			printSuffle = false;
			int Totalsuffle_numOutputTuples = 0;
			int Totalsuffle_numKeys = 0;
			for (vector<TReduce>::size_type m = 0; m != data_1->myObject->Reducers.size(); m++)
			{
				// data_1->myObject->Reducers.size()
				// Reducers[m]->Run()

				// printf("m: %li\n", m);
				printf("Suffle1  ->  \tnumOutputTuples:%i  \tnumProcessedKeys:%i \n",
					   data_1->myObject->Reducers[m]->GetSuffle_numOutputTuples(), data_1->myObject->Reducers[m]->GetSuffle_numKeys());
			}
			printf("\x1B[33m*** Total Suffle  ->  :\033[0m\n");
		}
		
		// **** Reduce ****
		//data_1->myObject->Reduce(data_1);
		if (data_1->myObject->Reduce(data_1) != COk)
			error("MapReduce::Concurent 1 - Error Reduce");
		pthread_barrier_wait(&EndReducer);
		if (printReduce == true)
		{
			printReduce = false;
			printf("\x1B[33m*** Total Reduce  ->  \tnumKeys:%i   \tnumOccurences:%i  \taverageOccurencesPerKey:%i  \tnumOutputBytes %i \033[0m\n",
				   TotalReduce_NumKeys, TotalReduce_NumOcur, TotalReduce_AvgOcur, TotalReduce_NumOutBytes);
		}
		
	}
	// pthread_mutex_unlock(&mutexStatistics);
}

TError
MapReduce::Reduce(struct thread_data_1 *data_1)
{
	// for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++)
	while (Reducers.size() != 0 && count_reduce != nreducers)
	{
		if (count_reduce != nreducers)
		{
			count_reduce++;
			pthread_mutex_lock(&Reduce_part);
			PtrReduce reductor = data_1->myObject->Reducers.back();
			// printf("Reducers size: %li\n", Reducers.size());
			data_1->myObject->Reducers.pop_back();
			pthread_mutex_unlock(&Reduce_part);
			reductor->Run();

			// Suffle Statistics (TODO: Poner en una funcion a parte)
			printf("Suffle2  ->  Thread:%ld   \tnumOutputTuples:%i  \tnumProcessedKeys:%i \n",
				   pthread_self(), reductor->GetSuffle_numOutputTuples(), reductor->GetSuffle_numKeys());

			// Reduce Statistics (TODO: Poner en una funcion a parte)
			printf("Reduce   ->  Thread:%ld   \tnumKeys:%i   \tnumOccurences:%i  \taverageOccurencesPerKey:%i  \tnumOutputBytes %i\n",
				   pthread_self(), reductor->GetReduce_numKeys(), reductor->GetReduce_numOccurences(), reductor->GetReduce_averageOccurKey(), reductor->GetReduce_numOutputBytes());

			pthread_mutex_lock(&Suffle_part);
			TotalReduce_NumKeys += reductor->GetReduce_numKeys();
			TotalReduce_NumOcur += reductor->GetReduce_numOccurences();
			TotalReduce_AvgOcur += reductor->GetReduce_averageOccurKey();
			TotalReduce_NumOutBytes += reductor->GetReduce_numOutputBytes();
			pthread_mutex_unlock(&Suffle_part);
		}
	}
	return (COk);

	/*for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++)
	{
		if (Reducers[m]->Run()!=COk)
			error("MapReduce::Reduce Run error.\n");
	}*/

	// pthread_mutex_lock(&Suffle_part);
	// printf("Reducer size:%li \n", data_1->myObject->Reducers.size());
	// while (!Reducers.empty() && count_reduce < nreducers)
	// {
	// 	printf("Thread: %ld\n", pthread_self());
	// 	PtrReduce reductor = data_1->myObject->Reducers.back();
	// 	data_1->myObject->Reducers.pop_back();
	// 	count_reduce++;
	// 	pthread_mutex_unlock(&Suffle_part);
	// 	reductor->Run();
	// }
}

TError
MapReduce::Suffle(struct thread_data_1 *data_1)
{
	TMapOuputIterator it2;
	// printf("Mappers size: %li\n", Mappers.size());

	while (Mappers.size() != 0 && count_suffle != numficheros)
	{

		if (count_suffle != numficheros)
		{
			count_suffle++;
			pthread_mutex_lock(&Suffle_part);
			multimap<string, int> output = Mappers.back()->getOutput();
			// printf("Mappers size: %li\n", Mappers.size());
			Mappers.pop_back();
			// printf("Pop %ld\n", pthread_self());
			pthread_mutex_unlock(&Suffle_part);

			for (TMapOuputIterator it1 = output.begin(); it1 != output.end(); it1 = it2)
			{
				TMapOutputKey key = (*it1).first;
				pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

				// Calcular a que reducer le corresponde está clave:
				int r = std::hash<TMapOutputKey>{}(key) % Reducers.size();

				if (debug)
					printf("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				pthread_mutex_lock(&mutex1);
				Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
				pthread_mutex_unlock(&mutex1);
				// Eliminar todas las entradas correspondientes a esta clave.
				// for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
				//   output.erase(it2);
				output.erase(keyRange.first, keyRange.second);
				it2 = keyRange.second;
			}
		}
	}

	// MyQueue <int> *sufflequeue = new MyQueue<int>[Reducers.size()];
	// sufflequeue.AddMap();
	// sufflequeue->push(4);
	// printf("MyQueue %s \n", sufflequeue.end);

	/*for (vector<TMap>::size_type m = 0; m != Mappers.size(); m++)
	{
		multimap<string, int> output = Mappers[m]->getOutput();

			// Process all mapper outputs
			for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
			{
				TMapOutputKey key = (*it1).first;
				pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

				// Calcular a que reducer le corresponde está clave:
				int r = std::hash<TMapOutputKey>{}(key)%Reducers.size();

				if (debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);

				// Eliminar todas las entradas correspondientes a esta clave.
				//for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
				//   output.erase(it2);
			output.erase(keyRange.first,keyRange.second);
			it2=keyRange.second;
			}
	}*/
	return (COk);
}

/*
void Fases_Concurentes_2(struct thread_data_2 *data_2)
{
	if (data_2->myObject->Reduce(data_2) != COk)
		error("MapReduce::Concurent 2 - Error Reduce");
}*/

// Retorna el numero de archivos en el directorio indicado por parametro
int getNumberFiles(char *input)
{
	int count = 0;
	DIR *dir;
	struct dirent *entry;
	if ((dir = opendir(input)) != NULL)
	{
		while ((entry = readdir(dir)) != NULL)
			if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && entry->d_type == 0x8)
				count++;
		closedir(dir);
	}
	else
	{
		perror("error leer directorio\n");
		return -1;
	}

	printf("\n\x1B[32m-> Number of input files: %i\033[0m\n", count);
	return count;
}

// Funcion para saber nombre del archivo que pasaremos a cada thread.
int getNamesFiles(char *names[], char *input)
{
	DIR *dir;
	struct dirent *entry;
	if ((dir = opendir(input)) != NULL)
	{
		int i = 0;
		while ((entry = readdir(dir)) != NULL)
		{
			if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && entry->d_type == 0x8)
			{
				names[i] = entry->d_name;
				i++;
			}
		}
		closedir(dir);
	}
	else
	{
		perror("error leer directorio\n");
		return -1;
	}
	return 0;
}

// Genera y lee diferentes splits: 1 split por fichero.
// Versión secuencial: asume que un único Map va a procesar todos los splits.
TError
MapReduce::Split(char *input, struct thread_data_1 *data_1)
{
	// printf("Split: thread %ld\n", pthread_self());
	if (data_1->map->ReadFileTuples(input) != COk)
	{
		error("MapReduce::Split Run error.\n");
	}
	return (COk);
}

// Ejecuta cada uno de los Maps.
TError
MapReduce::Map(struct thread_data_1 *data_1)
{
	// printf("Map: thread %ld\n", pthread_self());
	if (debug)
		printf("DEBUG::Running Map by thread number %ld\n", pthread_self());
	if (data_1->map->Run() != COk)
	{
		error("MapReduce::Map Run error.\n");
	}
	return (COk);
}

// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como
// función de partición, para distribuir las claves entre los posibles reducers.
// Utiliza un multimap para realizar la ordenación/unión.
/*TError
MapReduce::Suffle(struct thread_data_1 *data_1)
{
	printf("Shuffle: thread %ld\n", pthread_self());
	TMapOuputIterator it2;

	multimap<string, int> output = data_1->map->getOutput(); // data_1.map para coger nuestro mapa wey

	// Process all mapper outputs
	for (TMapOuputIterator it1 = output.begin(); it1 != output.end(); it1 = it2)
	{
		TMapOutputKey key = (*it1).first;
		pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

		// Calcular a que reducer le corresponde está clave:
		int r = std::hash<TMapOutputKey>{}(key) % Reducers.size();

		if (debug)
			printf("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

		// Añadir todas las tuplas de la clave al reducer correspondiente.
		pthread_mutex_lock(&mutex1);
		Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
		pthread_mutex_unlock(&mutex1);

		output.erase(keyRange.first, keyRange.second);
		it2 = keyRange.second;
	}
	return (COk);
}*/

// Ejecuta cada uno de los Reducers.
/*TError
MapReduce::Reduce(struct thread_data_2 *data_2)
{
	printf("Reduce: thread %ld\n", pthread_self());
	if (Reducers[data_2->numReducer]->Run() != COk)
	{
		error("MapReduce::Reduce Run error.\n");
	}
	return (COk);
}*/
