Real Time Anomaly Detection Framework
=================

This is also an on going project. The goal of this project is to design a anomaly detection framework to detect the contextual anomalies from a cluster of data streams in real time. The application scenarios include computing cluster monitoring, healthcare monitoring, environment monitoring, etc.

Generally, the anomaly detection can be conducted in three stages: Stream Dispatching, Anomaly Scoring and Alert Triggering. Stream Dispatching is an associate stage to enable the large scale anomaly detection. It is used to shuffle the data from streams to different computing components. The shuffling is conducted randomly, so the distribution of the data would preserved after shuffling. Anomaly Scoring is conducted in two steps, the Data Instance Scoring, and the Stream Scoring. The first step is to quantify the severity of anomaly for each data instance (the current observation) of a stream based on its deviation to the majority behaviors. The second is to quantify the severity of anomaly for each data streams combining its current its historical observations. Alert Triggering stage is to trigger the alert once a data stream is identified to be abnormal based its anomaly score. A data stream is identified as abnormal if its anomaly score is far from the median score of all the data streams. 

The wiki of this project is available [here](https://github.com/yxjiang/stream-outlier/wiki).
