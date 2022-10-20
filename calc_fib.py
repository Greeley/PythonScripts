"""
Name: calc_fib
Author: Dakota Carter
License: MIT
Description: used to find high value Fibonacci sequence numbers
"""
import time, json, os
super_cache = {'0': '0'}

def cmp_to_key(mycmp):
     'Convert a cmp= function into a key= function'
     class K:
         def __init__(self, obj):
             self.obj = obj
         def __lt__(self, other):
             return mycmp(self.obj, other.obj) < 0
         def __gt__(self, other):
             return mycmp(self.obj, other.obj) > 0
         def __eq__(self, other):
             return mycmp(self.obj, other.obj) == 0
         def __le__(self, other):
             return mycmp(self.obj, other.obj) <= 0
         def __ge__(self, other):
             return mycmp(self.obj, other.obj) >= 0
         def __ne__(self, other):
             return mycmp(self.obj, other.obj) != 0
     return K

def filename_compare(x, y):
     file1 = x.split('_')
     file2 = y.split('_')
     return int(file2[-1].strip('.json')) - int(file1[-1].strip('.json'))

def load_initial_cost(x:int):
    global super_cache
    if not os.path.exists('cache'):
        os.makedirs('cache')
        return x
    else:
        cache_files = os.listdir('cache')
        cache_files = sorted(cache_files, key=cmp_to_key(filename_compare))
        with open(os.path.join('cache', cache_files[0]), 'r') as cache:
            super_cache.clear()
            super_cache.update(json.load(cache))
        cache.close()
        return int(cache_files[0].split('_')[-1].strip('.json'))

def write_cache():
    global super_cache
    super_cache_keys = list(super_cache.keys())
    if len(super_cache_keys) % 10000 == 0:
        # Flush cache
        with open(os.path.join('cache',"{}_{}.json".format(super_cache_keys[0], super_cache_keys[-1])), 'w+') as cache:
            json.dump(super_cache, cache, separators=[', ', ': '], indent=4, sort_keys=False)
        cache.close()
        temp_cache = dict()
        temp_cache[super_cache_keys[-1]] = super_cache[super_cache_keys[-1]]
        temp_cache[super_cache_keys[-2]] = super_cache[super_cache_keys[-2]]
        super_cache = temp_cache

def read_cache(n):
    if os.path.exists('cache'):
        for cache_file in os.listdir('cache'):
            numbers = cache_file.split('_')
            if int(numbers[0]) == int(n):
                super_cache.clear()
                super_cache.update(json.load(open(os.path.join('cache', cache_file), 'r')))



def super_fib(n:str) -> str:
    try:
        fibn = super_cache.get(n)
        if fibn is None:
            if int(n) == 1:
                super_cache[n] = '1'
                if super_cache.get(str(int(n)+1)) is None:
                    super_cache[str(int(n)+1)] = '1'
                return '1'
            else:
                fibn = super_add(super_cache[str(int(n)-1)], super_cache[str(int(n)-2)])
                super_cache[n] = fibn
                if super_cache.get(str(int(n)+1)) is None:
                    super_cache[str(int(n)+1)] = super_add(fibn, super_cache[str(int(n)-1)])
                return fibn
        else:
            if super_cache.get(str(int(n) + 1)) is None:
                super_cache[str(int(n)+1)] = super_add(fibn, super_cache[str(int(n)-1)])
            return fibn
    except RecursionError as e:
        print("Couldn't calculate passed:", n)


def super_add(temp1:str, temp2:str) -> str:
    rollover = '0'
    fibn = list()
    diff = len(temp1) - len(temp2)
    if diff > 0:
        temp2 = ('0'*diff) + temp2
    elif diff < 0:
        temp1 = ('0'*abs(diff)) + temp1
    for z,x in zip(reversed(temp1), reversed(temp2)):
        sum = str(int(z) + int(x) + int(rollover))
        if len(sum) >= 2:
            fibn.insert(0, sum[-1])
            rollover = sum[:-1]
        else:
            fibn.insert(0, sum)
            rollover = '0'
    if int(rollover) > 0:
        fibn.insert(0, rollover)
        return "".join(fibn)
    else:
        return "".join(fibn)

if __name__ == '__main__':
    super_start = time.time()
    x = load_initial_cost(1)
    while True:
        print(x)
        read_cache(x)
        super_fib(str(x))
        write_cache()
        x+=1
    # print("\nSuper Fib:", "{:.5f}".format(time.time()-super_start), 'Seconds')


