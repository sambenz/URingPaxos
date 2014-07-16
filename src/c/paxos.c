/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <jni.h>
#include <stdlib.h>
#include <stdio.h>

#define BUFFER_LENGTH 56000
#define BUFFERS 15000

jbyte **buf;
int *len;
long *inst;

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    init
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_init
  (JNIEnv *env, jobject object){
    len = calloc(BUFFERS, sizeof(int));
    inst = calloc(BUFFERS, sizeof(long));
    buf = calloc (BUFFERS, sizeof(jbyte *)); //make array of arrays
    if(buf == NULL){
      // calloc failed
    }
    int i;
    for (i = 0; i < BUFFERS; i++) {
      buf[i] = calloc(BUFFER_LENGTH, sizeof(jbyte)); // make actual arrays
      if(buf[i] == NULL){
	// calloc failed
      }
    }
    return BUFFERS;
}

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    nput
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_nput
  (JNIEnv *env, jobject object, jlong instance, jbyteArray data){
    int key = instance % BUFFERS;
    inst[key] = instance;
    len[key]=(*env)->GetArrayLength(env,data);
    (*env)->GetByteArrayRegion(env,data,0,len[key],buf[key]);
}

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    nget
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_nget
  (JNIEnv *env, jobject object, jlong instance){
    int key = instance % BUFFERS;
    jbyteArray result;
    result = (*env)->NewByteArray(env, len[key]);
    if (result == NULL) {
      return NULL; /* out of memory error thrown */
    }
    (*env)->SetByteArrayRegion(env, result, 0, len[key], buf[key]);
    return result;
}

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    containsDecision
 * Signature: (Ljava/lang/Long;)Z
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_containsDecision
  (JNIEnv *env, jobject object, jobject instance){
   jclass Long = (*env)->GetObjectClass(env,instance);
   long i = (long)(*env)->GetIntField(env,instance,(*env)->GetFieldID(env,Long,"value","J"));
   int key = i % BUFFERS;
   if(inst[key] == i){
      return JNI_TRUE;
   }else{
      return JNI_FALSE;
   }
}
