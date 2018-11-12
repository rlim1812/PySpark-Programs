import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''

    conf = SparkConf().setAppName("SumOfNumbers").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    prime_nums = sc.textFile("in/prime_nums.text")
    prime_nums_list = prime_nums.flatMap(lambda line: line.split())
    prime_nums_sum = prime_nums_list.reduce(lambda x, y: int(x) + int(y))

    print("The sum of the first 100 prime numbers is: " + str(prime_nums_sum))
