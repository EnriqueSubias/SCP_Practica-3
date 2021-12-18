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
#include <stack>
#include "MyQueue.h"
using namespace std;

class MapReduce
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;

	vector<PtrMap> Mappers;


public:
	MapReduce(char *input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers);
	TError Run(int nreducers2);
	vector<PtrReduce> Reducers;
	int init_mutex();
	int destroy_mutex();
	int init_barrier(int nfiles,int nreducer);
	int destroy_barrier();

	TError Split(char *input, struct thread_data *data);
	TError Map(struct thread_data *data);
	TError Suffle(struct thread_data *data);
	TError Reduce(struct thread_data *data);

	inline void AddMap(PtrMap map) { Mappers.push_back(map); };
	inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
	// inline PtrMap PopStackMap(PtrMap map) { stackMaps.pop(); };
	// inline void TopStackMap(PtrMap map) { stackMaps.top(); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;

#endif /* MAPREDUCE_H_ */
