/* ---------------------------------------------------------------
Práctica 3.
Código fuente: MapReduce.h
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"
#include <stdio.h>
#include <functional>
#include <string>

class MapReduce
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;

	vector<PtrMap> Mappers;
	vector<PtrReduce> Reducers;

public:
	MapReduce(char *input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers);
	TError Run(int nreducers);

	TError Split(char *input, struct thread_data_1 *data_1);
	TError Map(struct thread_data_1 *data_1);
	TError Suffle(struct thread_data_1 *data_1);
	TError Reduce(struct thread_data_2 *data_2);

	inline void AddMap(PtrMap map) { Mappers.push_back(map); };
	inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;

#endif /* MAPREDUCE_H_ */
