#!/usr/bin/env python

'''
Developed and tested for Python 2.7

Prerequisites: 
    pip2.7 install happybase
    pip2.7 install bitarray

Happy Based connects to HBase through HBase thrift server

    # hbase thrift start
'''

import happybase
from bitarray import bitarray
import base64

_hbase_host = "localhost"
_hbase_conn = happybase.Connection(_hbase_host)

# def f(): 
#     a = bitarray(10)
#     a.setall(0)

def ndim_geohash(ranges, digits_per_dim, value):
    '''
    :param ranges: list of number pairs with the value ranges per dimension. For 
    each pair (l, u) we should have that l is the lower bound and u the upper
    bound, and l < u is True
    :param digits_per_dim: number of digits to add to the hash per dimension
    :param value: list of numbers corresponding to a vector of the same size 
    as ranges. 

    :returns a bitarray with the n-dimensional geohash of the vector represented 
    by value.
    '''
    # check preconditions
    for ran in ranges:
        assert ran[1] > ran[0], "empty or unitary ranges are allowed: {ran}".format(ran=ran)
    assert len(value) == len(ranges), "value {val} should have the same number of dimensions as {ranges}".format(val=value, ranges=ranges)
    # convert ranges to float lists to use float division in splits, and to allow mutation
    ranges = map(lambda (x, y):  [float(x), float(y)], ranges)
    # compute result
        # https://pypi.python.org/pypi/bitarray/ "Here, the high-bit comes first because big-endian means "most-significant first""
    pre_hash = bitarray(len(ranges) * digits_per_dim, endian='big')
    idx = 0
    for digit in xrange(digits_per_dim):
        for dim in xrange(len(ranges)):
            split_point = (ranges[dim][0] + ranges[dim][1]) / 2
            if value[dim] < split_point:
                pre_hash[idx] = 0
                ranges[dim][1] = split_point
            else: 
                pre_hash[idx] = 1
                ranges[dim][0] = split_point
            idx += 1

    return pre_hash 

def base32_ndim_geohash(ranges, digits_per_dim, value):
    '''
    Converts to base32 encoding as returned by computing ndim_geohash, converting
    it into byte with tobytes and then applying  base64.b32encode.

    According to https://pypi.python.org/pypi/bitarray/ "when the length of 
    the bitarray is not a multiple of 8, the few remaining bits (1..7) are set to 0",
    in this case the lenght is len(ranges) * digits_per_dim, i.e., the number of 
    dimensions times the number of digits per dimension
    '''
    return base64.b32encode(ndim_geohash(ranges, digits_per_dim, value).tobytes())

def base16_ndim_geohash(ranges, digits_per_dim, value):
    '''
    Converts to base16 encoding as returned by computing ndim_geohash, converting
    it into byte with tobytes and then applying  base64.b16encode.

    According to https://pypi.python.org/pypi/bitarray/ "when the length of 
    the bitarray is not a multiple of 8, the few remaining bits (1..7) are set to 0",
    in this case the lenght is len(ranges) * digits_per_dim, i.e., the number of 
    dimensions times the number of digits per dimension
    '''
    return base64.b16encode(ndim_geohash(ranges, digits_per_dim, value).tobytes())

if __name__ == '__main__':
    ranges, point = [(-180, 180), (-90, 90)], (-73.97, 40.78)
    precision = 3
    print ndim_geohash(ranges, precision, point)
    print ndim_geohash(ranges, precision, point) == bitarray('011001', endian='big')
    print base32_ndim_geohash(ranges, precision, point)
    print base16_ndim_geohash(ranges, precision, point)

    print ''
    print '''Note how the rigth zero padding introduced by calling tobytes() on bitarray
objects with lenght not multiple of 8 makes that prefixes are not respected when 
increasing the precission 
    '''
    precision = 5
    print ndim_geohash(ranges, precision, point)
    print ndim_geohash(ranges, precision, point) == bitarray('0110010111', endian='big')
    print base32_ndim_geohash(ranges, precision, point)
    print base16_ndim_geohash(ranges, precision, point)

    print ''
    print '''the common prefix failure in 32 bit encoding is also
due to not providing enough bytes 
    '''
    ranges, point = [(-180, 180), (-90, 90)], (-73.97, 40.78)
    precision = 8 / 2
    print ndim_geohash(ranges, precision, point)
    print base32_ndim_geohash(ranges, precision, point)
    print base16_ndim_geohash(ranges, precision, point)

    print ''
    precision = 16 / 2
    print ndim_geohash(ranges, precision, point)
    print base32_ndim_geohash(ranges, precision, point)
    print base16_ndim_geohash(ranges, precision, point)



 # base64.b64decode(b'\x00\x01\x01\x00\x00\x01'.encode('base64'))
 # base64.b64decode(b'\x00\x01\x01\x00\x00\x01'.encode('base64')) == b'\x00\x01\x01\x00\x00\x01'
