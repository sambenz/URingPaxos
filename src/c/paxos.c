#include <jni.h>
#include <stdlib.h>
#include <stdio.h>

#define BUFFER_LENGTH 56000
#define BUFFERS 15000

jbyte **buf;
int *len;
int *inst;

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    init
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_init
  (JNIEnv *env, jobject object){
    len = calloc(BUFFERS, sizeof(int));
    inst = calloc(BUFFERS, sizeof(int));    
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
 * Signature: (I[B)V
 */
JNIEXPORT void JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_nput
  (JNIEnv *env, jobject object, jint instance, jbyteArray data){
    int key = instance % BUFFERS;
    inst[key] = instance;
    len[key]=(*env)->GetArrayLength(env,data);
    (*env)->GetByteArrayRegion(env,data,0,len[key],buf[key]);
}

/*
 * Class:     ch_usi_da_paxos_storage_CyclicArray
 * Method:    nget
 * Signature: (I)[B
 */
JNIEXPORT jbyteArray JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_nget
  (JNIEnv *env, jobject object, jint instance){
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
 * Method:    contains
 * Signature: (Ljava/lang/Integer;)Z
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_da_paxos_storage_CyclicArray_contains
  (JNIEnv *env, jobject object, jobject instance){
   jclass Integer = (*env)->GetObjectClass(env,instance);
   int i = (int)(*env)->GetIntField(env,instance,(*env)->GetFieldID(env,Integer,"value","I"));
   int key = i % BUFFERS;
   if(inst[key] == i){
      return JNI_TRUE;
   }else{
      return JNI_FALSE;
   }
}
