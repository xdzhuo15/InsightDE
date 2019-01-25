# InsightDE
Multi-tasking ML: the DE approach

Problem Definition

Machine learning models are usually trained for performing one specific task well, however, most of human applications require good performance on multiple tasks. For example, radiologists need to get trained to identify different medical conditions such as fractured bones, tumor, or osteoporosis from the same one X-ray picture. So far, machine learning models, especially AI, significantly reduces performance when being trained on multi-tasking, and we need to build multiple engines for each specific task and perform them in a parallel fashion. If each of the model is large, such as convolutional neural network, and/or the amount of the data to be processed is large (~GB/s), and/or there are different work flows for each of the function, we need to engineer a scalable and fault tolerant system to deploy the models in scale, to schedule workflow and maintanence, and to track changes and catch errors.

In this Insight Data Engineering project, my goal is to fully explore the engineering challenges of deploying multiple machine learning models at the same time to accomplish an overaching goal, test the performance of the system, and develop roadmap for scaling and optimization. This project will build on the ImageNet and it's pretrained model Inception 3V, because of the large data size and extensive literature and availability of different machine learning models for object identification. Images from OpenImage, another datset as frequently used as ImageNet, will be used as streaming data for the pretrained models to process and identify multiple projects at the same time. Although Inception 3V already can classify objects into categories, in order to identify very specific objects, new models have to be trained with new data as well as features from Inception 3V. Therefore, we have a complex system of multiple operations, large data streaming, and database utilization.

Schematics

Datas and models needed

Inception 3V is running on tensor flow
2-3 models of new categories based on Inception 3V a) a fast model on MSN shopping image: https://www.kernix.com/blog/image-classification-with-a-pre-trained-deep-neural-network_p11 b) high accuracy of identifying dogs and cats: https://medium.com/abraia/first-steps-with-transfer-learning-for-custom-image-classification-with-keras-b941601fcad5 c) identifying flowers: https://www.tensorflow.org/hub/tutorials/image_retraining
ImageNet data (150GB)
Open Image data (500GB)
Techstacks

Airflow: schedule the validation of existing model, and retrain if needed
Kafka: data ingetion of streaming Open Image data
Flink: distributed computing with model
Spark: batch processing for retraining model
Flask: UI showing performance
EC2 for database
Engineering Challenge

Data injection: how to handle large and highly variable load
Time lag of different models before consensus: storage or even the computing?
Maintain different versions of ML models
Fault tolerant and catch error in time
Model output quality consistency before and after retraining
MVP

Performace: data injection speed, variability, process speed, memory use
Model evaluation: histogram of each category in training data vs. testing data
