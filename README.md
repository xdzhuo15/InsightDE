FreqEncoder
A Customized Pyspark Transformer for Streamlined Machine Learning Pipelines

Problem statement:
In this Insight Data Engineering project, I used spark transformers to build a data cleaning, feature engineering and machine learning pipleine to scale the Microsoft Malware Prediction algorithm. The benefits of using the transformers in the spark libraries include high readability of modularized code, streamlined process to reduce errors, and reductions of repartitioning and bottleneck without completely reinveting the wheel. However, spark only offers limited data cleaning and feature engineering transformer packages, which means we need to cutomize transformers accroding to the needs of our algorithms.

In this project, I customized a freqnecy encoder (FreqEncoder) based on the existing StringIndexer with pyspark. This transformer converts each unique value of a categorical variable into its respective counts in the training data, or the frequencies of occurence. Compared to StringIndexer which converts categorical variables into a pre-assigned number based on alphabetical order or sorted frequency, FreqEncoder captures the variability of the data without enforcing additional assumptions. This can be extremely useful when the categories inside the data have no particular seniority or priority relationship.

Tech Stack:
To build a production ready machine learning pipeline, I used spark as the powerhouse for both training and real-time predctions. I used Kafka to simulate input of thousands of users and stream the data to spark for real-time prediction. I used MySQL as the data sink and flask with dash for results visulization. The web UI compares the distribution of training and prediction data of top 10 features as well as the malware outcomes (has or no detections), so thst we can know when the model needs retraining.

The functions of FreqEncoder:




