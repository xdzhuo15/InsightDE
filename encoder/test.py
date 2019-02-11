from custom_spark_ml.feature.freqencoder import FreqEncoder, FreqEncoderModel

data = [
    ("Tax", 3),
    ("Tax", 3),
    ("Food", 1),
    ("Moon", 2),
    ("Tax", 3),
    ("Moon", 2),
    ("10.0", 1)
]

df = spark.createDataFrame(data, ["input", "expected"])

freqencoder = FreqEncoder() \
    .setInputCol("input") \
    .setOutputCol("bin")

model = freqencoder.fit(df)

model.transform(df).show()
