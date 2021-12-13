#include "MapReduce.h"
#include "Types.h"
#include <stdio.h>
#include <dirent.h>
#include <string.h>

using namespace std;
int splitBigFiles(char *input);
int getNumberFiles(char *input);
int getNamesFiles(char *names[], char *input);
void Fases_Concurentes_1(struct thread_data_1 *data_1);
void Fases_Concurentes_2(struct thread_data_2 *data_2);

struct thread_data_1
{
	char input_folder[256];
	char input_path[256];
	//int start_line, end_line;
	MapReduce *myObject;
	PtrMap map;
};

struct thread_data_2
{
	int numReducer;
	MapReduce *myObject;
};

pthread_mutex_t mutex;

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
MapReduce::Run(int nreducers)
{
	int nfiles = getNumberFiles(InputPath); // numero de los ficheros para crear los arrays

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
		printf("Input path %s: \n", data_1[i].input_path);
	}

	// Primeros threads
	printf("\n\x1B[32mProcesando Fase 1 con %d threads...\033[0m\n", nfiles);
	for (int i = 0; i < nfiles; i++)
	{
		if (debug)
			printf("> Thread %d creado Fase 1 \n", i);

		int s = pthread_create(&thread_ids_1[i], NULL, (void *(*)(void *))Fases_Concurentes_1, (void *)&data_1[i]);
		if (s != 0)
		{
			//error("pthread_create");
			error("Run::Concurent 1 - Error Create Thread");
		}
	}
	for (int i = 0; i < nfiles; i++)
	{
		if (debug)
			printf("> Thread %d join Fase 1\n", i);
		pthread_join(thread_ids_1[i], (void **)NULL);
	}

	// Segundos threads
	printf("\n\x1B[32mProcesando Fase 2 con %d threads...\033[0m\n", nreducers);
	for (int i = 0; i < nreducers; i++)
	{
		if (debug)
			printf("> Thread %d creado Fase 2 \n", i);

		int s = pthread_create(&thread_ids_2[i], NULL, (void *(*)(void *))Fases_Concurentes_2, (void *)&data_2[i]);
		if (s != 0)
		{
			error("Run::Concurent 2 - Error Create Thread");
		}
	}
	for (int i = 0; i < nreducers; i++)
	{
		if (debug)
			printf("> Thread %d join Fase 2\n", i);
		pthread_join(thread_ids_2[i], (void **)NULL);
	}

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

	if (data_1->myObject->Split(full_path, data_1) == COk)
	{
		if (data_1->myObject->Map(data_1) == COk)
		{
			if (data_1->myObject->Suffle(data_1) == COk)
			{
			}
			else
			{
				error("MapReduce::Concurent 1 - Error Shuffle");
			}
		}
		else
		{
			error("MapReduce::Concurent 1 - Error Map");
		}
	}
	else
	{
		error("MapReduce::Concurent 1 - Error Split");
	}
}

void Fases_Concurentes_2(struct thread_data_2 *data_2)
{
	if (data_2->myObject->Reduce(data_2) != COk)
		error("MapReduce::Concurent 2 - Error Reduce");
}

//Retorna el numero de archivos en el directorio indicado por parametro
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

//Funcion para saber nombre del archivo que pasaremos a cada thread.
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
	printf("Split: thread %ld\n", pthread_self());
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
	printf("Map: thread %ld\n", pthread_self());
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
TError
MapReduce::Suffle(struct thread_data_1 *data_1)
{
	printf("Shuffle: thread %ld\n", pthread_self());
	TMapOuputIterator it2;

	multimap<string, int> output = data_1->map->getOutput(); //data_1.map para coger nuestro mapa wey

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
		pthread_mutex_lock(&mutex);
		Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
		pthread_mutex_unlock(&mutex);

		output.erase(keyRange.first, keyRange.second);
		it2 = keyRange.second;
	}
	return (COk);
}

// Ejecuta cada uno de los Reducers.
TError
MapReduce::Reduce(struct thread_data_2 *data_2)
{
	printf("Reduce: thread %ld\n", pthread_self());
	if (Reducers[data_2->numReducer]->Run() != COk)
	{
		error("MapReduce::Reduce Run error.\n");
	}
	return (COk);
}
