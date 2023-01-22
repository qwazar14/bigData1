from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length


class ScotlandCities:
    def __init__(self):
        # self.spark = SparkSession.builder.appName("ScotlandCities").getOrCreate()
        conf = SparkConf()
        conf.setMaster("local").setAppName('My app')
        sc = SparkContext.getOrCreate(conf=conf)
        self.spark = SparkSession(sc)
        # self.spark = SparkSession.builder \
        #     .appName("aaa:") \
        #     .config("hive.exec.dynamic.partition", "true") \
        #     .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        #     .enableHiveSupport() \
        #     .getOrCreate()

        self.cities = ["Aberdeen", "Edinburgh", "Glasgow", "Inverness", "Dundee", "Stirling", "Perth"]

    def create_dataframe(self):
        # RDD зі списку міст
        cities_rdd = self.spark.sparkContext.parallelize(self.cities)

        # Застосуємо функцію map, щоб отримати новий RDD, що містить пари довжини міст
        cities_lengths_rdd = cities_rdd.map(lambda x: (x, len(x)))

        # Перетворимо RDD в DataFrame
        self.cities_df = self.spark.createDataFrame(cities_lengths_rdd, ["city", "length"])

    def show_result(self):
        self.cities_df.show()

    def count_rows(self):
        # Рахуємо кількість рядків у фреймі даних
        print("Number of rows: ", self.cities_df.count())

    def delete_column(self):
        # Видаляємо стовпець з назвою "length"
        self.cities_df = self.cities_df.drop("length")

    def convert_to_json(self):
        # Перетворимо формат DataFrame у формат JSON
        cities_json = self.cities_df.toJSON().collect()
        print(cities_json)

    def filter_rows(self):
        # Залишимо тільки рядки стовпця "city" з довжиною 4 або 5
        self.cities_df = self.cities_df.filter((length(col("city")) == 4) | (length(col("city")) == 5))

    def rename_column(self):
        # Вибираємо стовпець "city", перейменуємо його на "mid_length"
        self.cities_df = self.cities_df.selectExpr("city as mid_length")

    def delete_row(self):
        # Видаляємо рядок зі словом "Dundee"
        self.cities_df = self.cities_df.filter(col("mid_length") != "Dundee")

    def sort_alphabetically(self):
        # Відсортуймо назви міст за алфавітом
        self.cities_df = self.cities_df.sort("mid_length")

    def filter_and_sort_by_length(self):
        # Збережемо назви міст з кількістю літер у назві >= 6 і відсортуємо назви міст за зростанням довжини назви
        self.cities_df = self.cities_df.filter(length("mid_length") >= 6).sort(length("mid_length"), ascending=True)

    def pair_by_length(self):
        # Згрупуйємо назви міст за довжиною їхніх назв і оберімо чотири такі пари
        self.cities_df = self.cities_df.rdd.groupBy(lambda x: len(x[0])).take(4)
        print(self.cities_df)

    def sort_descending_length(self):
        # Відсортуємо назви міст у порядку спадання довжини їхніх назв
        self.cities_df = self.cities_df.sort(length("mid_length"), ascending=False)

    def count_by_length(self):
        # Визначимо, скільки слів існує для кожної довжини слова
        self.cities_df = self.cities_df.groupBy(length("mid_length")).count()


scotland_cities = ScotlandCities()
scotland_cities.create_dataframe()
scotland_cities.show_result()
scotland_cities.count_rows()
scotland_cities.delete_column()
scotland_cities.show_result()
scotland_cities.convert_to_json()
scotland_cities.filter_rows()
scotland_cities.rename_column()
scotland_cities.show_result()
# scotland_cities.delete_row()
# scotland_cities.show_result()
# scotland_cities.sort_alphabetically()
# scotland_cities.show_result()
scotland_cities.filter_and_sort_by_length()
scotland_cities.show_result()
scotland_cities.pair_by_length()
scotland_cities.sort_descending_length()
scotland_cities.show_result()
scotland_cities.count_by_length()
