/* ---------------------------------------------------------------
Práctica 3.
Código fuente: MyQueue.cpp
Grau Informàtica
X5707036T Robert Dragos Trif Apoltan
49271860T Enrique Alejo Subías Melgar
--------------------------------------------------------------- */

//#include "MyQueue.h"

//#include <queue>

#include <pthread.h>

template <class T>
MyQueue<T>::MyQueue()
{
	//	Queue = new std::queue<T>();
	pthread_rwlock_init(&rwlock, NULL);
}

//template class MyQueue<int>;