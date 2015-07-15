# -*- coding: utf-8 -*-
'''
Created on 2015年6月28日
布隆过滤器
@author: liqing
'''
import BitVector
import cmath


def is_prime(n):
    '''是否是素数'''
    if n == 9:
        return False
    for i in range(3,int(n**0.5)):
        if n%i == 0:
            return False
    return True

def find_prime(n): 
    '''找到从5开始的n个素书，使用素数筛法'''
    prime = []
    i = 5
    while len(prime) != n:
        flag = False
        for j in prime:
            if i % j == 0:
                flag = True
                i += 1
                break
        if flag: #如果能被素书整除就跳过一轮循环
            continue

        if is_prime(i):
            prime.append(i)
        i += 1
    return prime

class SimpleHash():  
    """这里使用了bkdrhash的哈希算法"""
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed
    
    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed*ret + ord(value[i])
        return (self.cap-1) & ret    #控制哈系函数的值域
    
    
class BloomFilter():
    '''     类名是Bloom_Fileter，初始化时传入数据的数量
    mark_value 函数是用于标记值的函数，应传入想要标记的值
    exists 函数是用于检测某个值是否已经被标记的函数，应传入想要检测的值'''

    def __init__(self, amount = 1 << 26):
        self.containerSize = (-1) * amount * cmath.log(0.001) / (cmath.log(2) * cmath.log(2)) #计算最佳空间大小
        self.containerSize = int(self.containerSize.real) #取整

        self.hashAmount = cmath.log(2) * (self.containerSize) / (amount)
        self.hashAmount = int(self.hashAmount.real)

        self.container = BitVector.BitVector(size = int(self.containerSize)) #分配内存

        self.hashSeeds = find_prime(self.hashAmount)

        self.hash = []
        for i in range(int(self.hashAmount)): #生成哈希函数
            self.hash.append(SimpleHash(self.containerSize, self.hashSeeds[i]))

        return 

    def exists(self, value):
        '''存在返回真，否则返回假'''
        if value == None:
            return False 
        for func in self.hash :
            if self.container[func.hash(str(value))] == 0 :
                return False
            return True

    def markValue(self, value):
        '''value是要标记的元素'''
        for func in self.hash :
            self.container[func.hash(str(value))] = 1
        return 
    

