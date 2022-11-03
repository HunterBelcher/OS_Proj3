#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pzip.h"

pthread_barrier_t barrier;
pthread_mutex_t char_frequency_lock;

/**
 * pzip() - zip an array of characters in parallel
 *
 * Inputs:
 * @n_threads:		   The number of threads to use in pzip
 * @input_chars:		   The input characters (a-z) to be zipped
 * @input_chars_size:	   The number of characaters in the input file
 *
 * Outputs:
 * @zipped_chars:       The array of zipped_char structs
 * @zipped_chars_count:   The total count of inserted elements into the zippedChars array.
 * @char_frequency[26]: Total number of occurences
 *
 * NOTE: All outputs are already allocated. DO NOT MALLOC or REASSIGN THEM !!!
 *
 */
void pzip(int n_threads, char *input_chars, int input_chars_size,
	  struct zipped_char *zipped_chars, int *zipped_chars_count,
	  int *char_frequency)
{
  int chars_per_thread = input_chars_size / n_threads;
  pthread_t thread_ids[n_threads];
  thread_args arg_array[n_threads];
  int zipped_counts[n_threads];

  for(int i=0; i<n_threads;i++)
  {
    thread_ids[i] = 0;
    zipped_counts[i] = 0;
  }

  if( pthread_barrier_init(&barrier, NULL, n_threads) || pthread_mutex_init(&char_frequency_lock, NULL))
  {
    fprintf(stderr, "A Syscall Failed");
    exit(1);
  }

  for (int i = 0; i<n_threads;i++)
  {
    arg_array[i].start_idx = (i) * chars_per_thread;
    arg_array[i].len = chars_per_thread;
    arg_array[i].input_chars = input_chars;
    arg_array[i].zipped_counts = zipped_counts;
    arg_array[i].char_frequency = char_frequency;
    arg_array[i].thread_idx = i;
    arg_array[i].zipped_chars = zipped_chars;


    if(pthread_create(&thread_ids[i], NULL, pack_section, &arg_array[i]))
    {
      fprintf(stderr, "A Syscall Failed");
      exit(1);
    }
  }

  for(int i = 0;i<n_threads;i++)
  {
    if(pthread_join(thread_ids[i], NULL))
    {
      fprintf(stderr, "A Syscall Failed");
      exit(1);
    }
    *zipped_chars_count += arg_array->zipped_counts[i];
  }
}

void *pack_section(void *arguments)
{
  thread_args *args;
  args = arguments;
  char last_char = args->input_chars[args->start_idx];
  if(pthread_mutex_lock(&char_frequency_lock))
  {
    fprintf(stderr, "A Syscall Failed");
    exit(1);
  }
  args->char_frequency[args->input_chars[args->start_idx] - 'a']++;
  if(pthread_mutex_unlock(&char_frequency_lock))
  {
    fprintf(stderr, "A Syscall Failed");
    exit(1);
  }
  int char_counter = 1;
  struct zipped_char local_zip[args->len];
  int zipped_arr_idx = 0;

  for(int i=args->start_idx + 1; i < args->start_idx + args->len; i++)
  {
    if(pthread_mutex_lock(&char_frequency_lock))
    {
      fprintf(stderr, "A Syscall Failed");
      exit(1);
    }
    args->char_frequency[args->input_chars[i] - 'a']++;
    if(pthread_mutex_unlock(&char_frequency_lock))
    {
      fprintf(stderr, "A Syscall Failed");
      exit(1);
    }
    if(last_char == args->input_chars[i])
    {
      char_counter++;
    }
    else
    {
      local_zip[args->zipped_counts[args->thread_idx]].character = last_char;
      local_zip[args->zipped_counts[args->thread_idx]].occurence = char_counter;
      args->zipped_counts[args->thread_idx]++;
      char_counter = 1;
      last_char = args->input_chars[i];
    }
  }
  local_zip[args->zipped_counts[args->thread_idx]].character = last_char;
  local_zip[args->zipped_counts[args->thread_idx]].occurence = char_counter;
  args->zipped_counts[args->thread_idx]++;
  char_counter = 1;
  pthread_barrier_wait(&barrier);
  for(int i = 0; i<args->thread_idx; i++)
  {
    zipped_arr_idx += args->zipped_counts[i];
  }
  for(int i = 0; i<args->zipped_counts[args->thread_idx]; i++)
  {
    args->zipped_chars[zipped_arr_idx].character = local_zip[i].character;
    args->zipped_chars[zipped_arr_idx].occurence = local_zip[i].occurence;
    zipped_arr_idx++;
  }
  return NULL;
}