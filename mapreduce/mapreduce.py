#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict

'''
Simple sequential mapreduce interpreter

Developed for python 2.7
'''
def uncurry(f):
    '''
    Converts a function with two arguments into a function that accepts a single pair as its argument
    '''
    return (lambda p: f(p[0], p[1]))

def concatMapIter(f, xs):
    '''
    Version with Python iterators of Haskell's concatMap
    '''
    return (v for vs in map(f, xs) for v in vs)

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

def print_mr_stats(combine_dict, shuffle_dict):
    print '\n', '-'*30
    print "combine_dict:"
    print "\t#keys:", len(combine_dict.keys()), "| contents:", dict(combine_dict)
    print "shuffle_dict:"
    print "\t#keys:", len(shuffle_dict.keys()), "| contents:", dict(shuffle_dict)
    print '-'*30, '\n'

def mapreduce(map_f = id_map_f, reduce_f = id_reduce_f, combine_f = id_reduce_f, verbose=False):
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
        combine_dict = shuffle(concatMapIter(uncurry(map_f), input_pairs))
        shuffle_dict = shuffle(concatMapIter(uncurry(combine_f), combine_dict.iteritems()))
        if verbose:
            print_mr_stats(combine_dict, shuffle_dict)
        return concatMapIter(uncurry(reduce_f), shuffle_dict.iteritems())
    return mr

def run_mapreduce(input_pairs, map_f = id_map_f, reduce_f = id_reduce_f, combine_f = id_reduce_f, verbose=False):
    return list(mapreduce(map_f=map_f, combine_f=combine_f, reduce_f=reduce_f, verbose=verbose)(input_pairs))
