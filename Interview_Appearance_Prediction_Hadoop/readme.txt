hadoop fs -mkdir dataset
hadoop fs -put Interview.csv dataset

hadoop fs -rm -r Ana*
hadoop fs -rm -r Prob*
hadoop fs -rm -r Ou*

hadoop jar IA.jar ani.IADriver
hadoop jar IA.jar test.TestDriver
hadoop jar IA.jar analysis.AnalysisDriver

