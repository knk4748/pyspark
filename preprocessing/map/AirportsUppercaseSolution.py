
from pyspark import SparkContext, SparkConf

class Utils():
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airportsRDD = sc.textFile("airports.text")

    airportPairRDD = airportsRDD.map(lambda line: \
        (Utils.COMMA_DELIMITER.split(line)[1], \
      Utils.COMMA_DELIMITER.split(line)[3]))

    upperCase = airportPairRDD.mapValues(lambda countryName: countryName.upper())

    upperCase.saveAsTextFile("out/airports_uppercase.text")
