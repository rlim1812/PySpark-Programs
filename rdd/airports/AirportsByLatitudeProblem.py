import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("in/airports.text")
    airportsInUSA = airports.filter(lambda line : float(Utils.COMMA_DELIMITER.split(line)[6]) > 40.0)

    airportsNameAndLatitude = airportsInUSA.map(splitComma)
    airportsNameAndLatitude.saveAsTextFile("out/airports_with_lat_greater_than_40.text")


