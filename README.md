Automate Malware Prediction System

Use Case: Cyber attacks happen once every 39 seconds, affecting one out of three American businesses each year, and the cost of a data breach averages at $1.3 million. In this Insight project, I used malware attack data collected from more than 16.8 million windows devices to build a real-time malware risk monitoring system for enterprises and mission-critical applications. I used a distributed system to productize and optimize predictive algorithms so that risk monitoring can be achieved reliably and effectively.

The vaule of the system not only lies in the ability to predict risk of malware attack but also the backend data analytics based on A/B testing to compare machines that have been detected vs. not detected, so that risk stratification and insights for computer volunerability can be derived by internal experts.

Data Source: Microsoft Malware Prediction, 10GB: https://www.kaggle.com/c/microsoft-malware-prediction

Assuming a system that runs its modole and update everyday, and we are looking at a data processing volume of 16.8million messages per day.

Technical Stacks:

Kafka streaming: simulate incoming information of new users
Spark streaming: predicts malware risks
Spark batch processing: regular retraining of prediction model
Airflow: scheduling retraining
Flask: prediction time, alerts of high risk incidences, training status, training time
Technical Challenges:

The system can handle large number of users with no latency (10M/day within seconds of response)
Automate training to ensure quality prediction
Performace tracking to help identify problems during calculation
