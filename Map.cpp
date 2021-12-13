#include "Map.h"
#include "Types.h"

#include <fstream> // std::ifstream

// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
// tuplas (key,value).
TError
Map::ReadFileTuples(char *fileName)
{

	ifstream file(fileName);
	string str;
	streampos Offset = 0;

	if (!file.is_open())
		return (CErrorOpenInputFile);

	while (std::getline(file, str))
	{
		if (debug)
			printf("DEBUG::Map input %d -> %s\n", (int)Offset, str.c_str());
		AddInput(new TMapInputTuple((TMapInputKey)Offset, str));
		Offset = file.tellg();
	}

	file.close();

	return (COk);
}

/* ············ Intento de implementar lo de los archivos de mas de 8MB ············
TError
Map::ReadFileTuples(char *fileName)//, int start_line, int end_line)
{
	//fileName = strcat("./Test/", fileName);
	printf("Estoy en MAP: %s\n", fileName);
	ifstream file(fileName);
	string str;
	streampos Offset = 0;
	printf("\x1B[32m  *  Test 01 ---> %i <--- %s \033[0m\n", pthread_self(), fileName);

	if (!file.is_open())
		return (CErrorOpenInputFile);

	*/
/*
	for (int i = 0; i < start_line; i++)
		getline(file, str);

	for (int i = start_line; i < end_line; i++)
	{
		std::getline(file, str);
		if (debug) printf("DEBUG:Thread: %i : \t Map input %d -> %s\n", pthread_self(), (int)Offset, str.c_str());
		AddInput(new TMapInputTuple((TMapInputKey)Offset, str));
		Offset = file.tellg();
	}*/
/*
	int i = 0;
	while (std::getline(file, str))
	{
		//if (debug)
		//printf("DEBUG:Thread: %i :\tMap input %d -> %s\n", pthread_self(), (int)Offset, str.c_str());
		AddInput(new TMapInputTuple((TMapInputKey)Offset, str));
		Offset = file.tellg();
		printf("[%d] ", i);
		i++;
	}
	printf("\n\x1B[32m  -  Test 02 ---> %i <--- %s \033[0m\n", pthread_self(), fileName);


	file.close();

	return (COk);
}*/

// tuplas (key,value).
void Map::AddInput(PtrMapInputTuple tuple)
{
	Input.push(tuple);
}

// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
// invoca a la función de Map especificada por el programador.
TError
Map::Run()
{
	TError err;

	while (!Input.empty())
	{

		if (debug)
			printf("DEBUG:Thread %ld :Map process input tuple %ld -> %s\n", pthread_self(), (Input.front())->getKey(), (Input.front())->getValue().c_str());
		err = MapFunction(this, *(Input.front()));
		if (err != COk)
			return (err);

		Input.pop();
	}

	return (COk);
}

// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
void Map::EmitResult(TMapOutputKey key, TMapOutputValue value)
{
	//if (debug)
	if (debug)
		printf("%ld DEBUG::Map emit result %s -> %d\n", pthread_self(), key.c_str(), value);
	Output.insert(TMapOuptTuple(key, value));
}
