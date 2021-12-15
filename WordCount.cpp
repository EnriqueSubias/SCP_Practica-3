/* ---------------------------------------------------------------
Práctica 3.
Código fuente: WordCount.cpp
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#include "Types.h"
#include "MapReduce.h"

#include <sstream>
#include <stdlib.h>

#include <stdlib.h>

using namespace std;

TError MapWordCount(PtrMap, TMapInputTuple tuple);
TError ReduceWordCount(PtrReduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end);

int main(int argc, char *argv[])
{
	char *input_dir, *output_dir;
	int num_reducers;

	printf("\n\033[3;104;30m ---- Inicio del programa ---- \033[0m\n\n");

	// Procesar argumentos.
	if (argc == 3)
	{
		num_reducers = 2;
		printf("\x1B[32m-> Number of reducers set to 2 by default\033[0m\n");
	}
	else if (argc == 4)
	{
		num_reducers = atoi(argv[3]);
		printf("\x1B[32m-> Reducers number: %d\033[0m\n", num_reducers);
	}
	else // if (argc < 3 || argc > 4)
	{
		error("Error in arguments: WordCount <input dir> <ouput dir> [num_reducers].\n");
	}

	input_dir = argv[1];
	output_dir = argv[2];

	PtrMapReduce mapr = new TMapReduce(input_dir, output_dir, MapWordCount, ReduceWordCount, num_reducers);
	if (mapr == NULL)
		error("Error new MapReduce.\n");

	mapr->Run(num_reducers);

	exit(0);
}

// Word Count Map.
TError MapWordCount(PtrMap map, TMapInputTuple tuple)
{
	string value = tuple.getValue();

	if (debug)
		printf("DEBUG::MapWordCount procesing tuple %ld->%s\n", tuple.getKey(), tuple.getValue().c_str());

	// Convertir todos los posibles separadores de palabras a espacios.
	for (int i = 0; i < value.length(); i++)
	{
		if (value[i] == ':' || value[i] == '.' || value[i] == ';' || value[i] == ',' ||
			value[i] == '"' || value[i] == '\'' || value[i] == '(' || value[i] == ')' ||
			value[i] == '[' || value[i] == ']' || value[i] == '?' || value[i] == '!' ||
			value[i] == '%' || value[i] == '<' || value[i] == '>' || value[i] == '-' ||
			value[i] == '_' || value[i] == '#' || value[i] == '*' || value[i] == '/')
			value[i] = ' ';
	}

	stringstream ss;
	string temp;
	ss.str(value);
	// Emit map result (word,'1').
	while (ss >> temp)
		map->EmitResult(temp, 1);

	return (COk);
}

// Word Count Reduce.
TError ReduceWordCount(PtrReduce reduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end)
{
	TReduceInputIterator it;
	int totalCount = 0;

	if (debug)
		printf("DEBUG::ReduceWordCount key %s ->", key.c_str());

	// Procesar todas los valores para esta clave.
	for (it = begin; it != end; it++)
	{
		if (debug)
			printf(" %d", it->second);
		totalCount += it->second;
	}

	if (debug)
		printf(".\n");

	reduce->EmitResult(key, totalCount);

	return (COk);
}
