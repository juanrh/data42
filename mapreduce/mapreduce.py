#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict
from random import randint
'''
Simple sequential mapreduce interpreter

Developed for python 2.7
'''
def uncurry(f):
    '''
    Converts a function with two arguments into a function that accepts a single pair as its argument
    '''
    return (lambda p: f(p[0], p[1]))

def concat_map(f, xs):
    '''
    Version with Python iterators of Haskell's concatMap
    '''
    return [v for vs in map(f, xs) for v in vs]

def concat(xss):
    return [x for xs in xss for x in xs]

def random_split(xs, n_parts):
    parts = [[] for i in xrange(0, n_parts)]
    for x in xs:
        parts[randint(0, n_parts-1)].append(x)
    return [part for part in parts if len(part) > 0]

def shuffle(kv_pairs_list):
    '''
    Given a list kv_pairs_list of (key, value) pairs, returns a dictionary from each key to a list of its corresponding values
    '''
    shuffle_dict = defaultdict(list)
    for k, v in kv_pairs_list:
        shuffle_dict[k].append(v)
    return shuffle_dict

def id_map_f(k, v):
    '''
    Identity map function
    '''
    yield((k, v))

def id_reduce_f(k, vs):
    '''
    Identity reduce funcion
    '''
    for v in vs:
        yield((k, v))

#def print_mr_stats(combine_dict, shuffle_dict):

def print_mr_stats(mappers_inputs, combiners_inputs, combiners_outputs, shuffled_pairs):
    print '\n', '-'*30
    print "mappers_inputs:"
    print "\t#mappers:", len(mappers_inputs), "| contents:", mappers_inputs
    print "combiners_inputs:"
    print "\t#keys:", map(lambda d: len(d.keys()), combiners_inputs), "| contents:", map(dict, combiners_inputs)
    print "combiners_outputs:"
    print "\t#pairs:", map(len, combiners_outputs), "| contents:", combiners_outputs
    print "shuffled_pairs:"
    print "\t#keys:", shuffled_pairs, "| contents:", dict(shuffled_pairs)
    print '-'*30, '\n'

def mapreduce(map_f = id_map_f, reduce_f = id_reduce_f, combine_f = id_reduce_f, num_mappers = 4,  verbose=False):
    '''
    Assumming the following types for the arguments

    map_f :: (k1, v1) -> iterable((k2, v2))
    reduce_f :: (k2, [v2]) -> iterable((k3, v3))
    combine_f :: (k2, [v2]) -> iterable((k2, v2))

    These functions take two arguments and return iterators of tuples. 
    This function returns a function mr :: iterable((k1, v1)) -> iterable((k2, v2)) that performs a simulation of a MapReduce computation. 

    If verbose is True then additional execution info will be printed to stdout at the end of each execution of the returning function mr
    '''
    def mr(input_pairs):
        mappers_inputs = random_split(input_pairs, num_mappers)
        combiners_inputs = [shuffle(concat_map(uncurry(map_f), input_pairs)) for input_pairs in mappers_inputs]
        combiners_outputs = [concat_map(uncurry(combine_f), combine_dict.iteritems()) for combine_dict in combiners_inputs] 
        shuffled_pairs = shuffle(concat(combiners_outputs))
        if verbose:
            print_mr_stats(mappers_inputs, combiners_inputs, combiners_outputs, shuffled_pairs)
        return concat_map(uncurry(reduce_f), shuffled_pairs.iteritems())
    return mr

def run_mapreduce(input_pairs, map_f = id_map_f, reduce_f = id_reduce_f, combine_f = id_reduce_f, verbose=False):
    return list(mapreduce(map_f=map_f, combine_f=combine_f, reduce_f=reduce_f, verbose=verbose)(input_pairs))

if __name__ == '__main__':
    def word_count(title_text_pairs, verbose=False):
        def map_f(_title, text):
            for word in text.split(' '):
                yield((word, 1))   

        def reduce_f(word, counts):
            yield((word, sum(counts)))

        return list(mapreduce(map_f=map_f, combine_f=reduce_f, reduce_f=reduce_f, verbose=verbose)(title_text_pairs))

    wc_input = [(None, "hola que tal hola"), (None, "ey hola"), (None, "como estamos")]
    print "Executing word_count for input:", wc_input
    print word_count(wc_input, verbose=True)
    print "\n\nResult not verbose:", word_count(wc_input, verbose=False)  
