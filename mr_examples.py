#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mapreduce import mapreduce

def word_count(title_text_pairs, verbose=False):
    def map_f(_title, text):
        for word in text.split(' '):
            yield((word, 1))   

    def reduce_f(word, counts):
        yield((word, sum(counts)))

    return mapreduce(map_f=map_f, combine_f=reduce_f, reduce_f=reduce_f, verbose=verbose)(title_text_pairs)

if __name__ == '__main__':
    wc_input = [("Moon", '"Never" does not exist for the human mind… only "Not yet."'),
                ("Spooner", "You must be the dumbest, smart person in the world. And you must be the dumbest, dumb person in the world."),
                ("Blake", "Easy! Take it easy! I hate personal violence, especially when I’m the person.")]
    print "Executing word_count for input:", wc_input
    print word_count(wc_input, verbose=True)
    print "\n\nResult not verbose:", word_count(wc_input, verbose=False)  
