from pyspark import SparkContext, SparkConf
import re

class Utils():
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    airports = sc.textFile("airports.text")
    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")