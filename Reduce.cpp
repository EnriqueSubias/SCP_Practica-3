/* ---------------------------------------------------------------
Práctica 3.
Código fuente: Reduce.cpp
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#include "Reduce.h"
#include "Types.h"
#include <fstream> // std::ifstream

// Constructor para una tarea Reduce, se le pasa la función que reducción que tiene que
// ejecutar para cada tupla de entrada y el nombre del fichero de salida en donde generará
// los resultados.

// Suffle  Statiscis
int suffle_numOutputTuples = 0; // Numero de tuplas de salida procesadas
int suffle_numKeys = 0;			// Numero de claves procesadas

// Reduce Statiscis
int reduce_numKeys = 0;			  // Numero de claves diferentes procesadas
int reduce_numOccurences = 0;	  // Numero de ocurrencias procesadas
float reduce_averageOccurKey = 0; // Valor medio ocurrencias/clave
int reduce_numOutputBytes = 0;	  // Numero bytes escritos de salida

pthread_mutex_t mutexLock;

int Reduce::init_mutex_lock()
{
	int err;
	err = pthread_mutex_init(&mutexLock, NULL);
	if (err != 0)
		// return TError(1);
		printf("\n mutex init failed\n");

	printf("err = %i\n", err);
	return COk;
	// */if (pthread_mutex_init(&mutexLock, NULL) != 0)
	// {
	//     printf("\n mutex init failed\n");
	// }*/
}

int Reduce::destroy_mutex_lock()
{
	pthread_mutex_destroy(&mutexLock);
	return COk;
}

Reduce::Reduce(TReduceFunction reduceFunction, string OutputPath)
{
	ReduceFunction = reduceFunction;
	// if (debug)
	printf("DEBUG::Creating output file %s\n", OutputPath.c_str());

	OutputFile.open(OutputPath, std::ofstream::out | std::ofstream::trunc);
	if (!OutputFile.is_open())
		error("Reduce::Reduce Error opening " + OutputPath + " output file.");
}

// Destructor tareas Reduce: cierra fichero salida.
Reduce::~Reduce()
{
	OutputFile.close();
}

// Función para añadir las tuplas de entrada para la función de redución en forma de lista de
// tuplas (key,value).
void Reduce::AddInputKeys(TMapOuputIterator begin, TMapOuputIterator end)
{
	TMapOuputIterator it;
	suffle_numKeys++;
	for (it = begin; it != end; it++)
	{
		AddInput(it->first, it->second);
	}
}

void Reduce::AddInput(TReduceInputKey key, TReduceInputValue value)
{
	pthread_mutex_lock(&mutexLock);
	if (debug)
		printf("DEBUG::Reduce add input %s -> %d\n", key.c_str(), value);
	suffle_numOutputTuples++;
	Input.insert(TReduceInputTuple(key, value));
	pthread_mutex_unlock(&mutexLock);
}

// Función de ejecución de la tarea Reduce: por cada tupla de entrada invoca a la función
// especificada por el programador, pasandolo el objeto Reduce, la clave y la lista de
// valores.
TError
Reduce::Run()
{
	TError err;
	TReduceInputIterator it2;

	// Process all reducer inputs
	for (TReduceInputIterator it1 = Input.begin(); it1 != Input.end(); it1 = it2)
	{
		TReduceInputKey key = (*it1).first;
		pair<TReduceInputIterator, TReduceInputIterator> keyRange = Input.equal_range(key);

		err = ReduceFunction(this, key, keyRange.first, keyRange.second);
		if (err != COk)
			return (err);

		// for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
		//    Input.erase(it2);
		Input.erase(keyRange.first, keyRange.second);
		it2 = keyRange.second;
	}
	reduce_averageOccurKey = float(reduce_numKeys) / float(reduce_numOccurences);
	return (COk);
}

// Función para escribir un resulta en el fichero de salida.
void Reduce::EmitResult(TReduceOutputKey key, TReduceOutputValue value)
{
	OutputFile << key << " " << value << endl;
	reduce_numOutputBytes += key.length();
	reduce_numOccurences += value;
	reduce_numKeys++;
}

// Shuffle
int Reduce::GetSuffle_numOutputTuples()
{
	return suffle_numOutputTuples;
}

void Reduce::PrintSuffle()
{
	printf("Suffle1  ->  \tnumOutputTuples:%i  \tnumProcessedKeys:%i \n",
		   suffle_numOutputTuples, suffle_numKeys);
}

int Reduce::GetSuffle_numKeys()
{
	return suffle_numKeys;
}

// Reduce
int Reduce::GetReduce_numKeys()
{
	return reduce_numKeys;
}

int Reduce::GetReduce_numOccurences()
{
	return reduce_numOccurences;
}

float Reduce::GetReduce_averageOccurKey()
{
	return reduce_averageOccurKey;
}

int Reduce::GetReduce_numOutputBytes()
{
	return reduce_numOutputBytes;
}
