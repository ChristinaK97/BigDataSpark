## Scalable Processing of Dominance-based Queries

This repository houses a solution for scalable processing of dominance-based queries in multidimensional data. 
The project leverages the power of Apache Spark for distributed computing and employs Aggregate R* Trees (aR*Trees) for efficient data indexing. 
Additionally, a baseline solution is also included to validate our results and benchmark the performance of our proposed methodology.


### Types of Dominance-based Queries
* __Q1 Skyline Computation__: Given a set of d-dimensional points, return the set of points that are not dominated. This is also
  known as the skyline set.
* __Q2 Top-k Dominating Points__: Given a set of d-dimensional points, return the k points with the highest dominance score. The
  dominance score of a point p is defined as the total number of points dominated by p.
* __Q3 Top-k Skyline Dominating Points__: Given a set of d-dimensional points, return the k points from the skyline with the highest dominance
  score.


### Key Features:

* __Data Indexing with R-Trees__<br> The project utilizes a specialized indexing structure, aR*Trees, designed for organizing and analyzing multidimensional spatial data. This serves as a foundation for scalable dominance-based query processing.
    <br> Navigate through the [```RStarTreeScala```](https://github.com/ChristinaK97/BigDataSpark/tree/dev_branch/RStarTreeScala) directory to access the Scala implementation of the index. Project organization:
    - *FileHandler*: Storage of the tree as a secondary memory index
    - *Geometry*: Geometric entities in a multidimensional space
    - *TreeStructure*: Fundamental building blocks of a tree data structure
    - *TreeFunctions/CreateTreeFunctions*: Implements operations for creating the tree index.


* __Algorithms Tailored to Indexed Data__<br> We provide algorithms specifically designed to leverage the aR*Trees. Notable implementations include:
  - __[Q1]__ A __Branch and Bound__ approach for skyline computation. <br>
    Navigate to [```RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SkylineBBS.scala```](https://github.com/ChristinaK97/BigDataSpark/blob/dev_branch/RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SkylineBBS.scala)
  - __[Q2]__ The __Simple Counting Guided__ algorithm for identifying the Top-k dominating points. <br>
    Navigate to [```RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SCG_TopK.scala```](https://github.com/ChristinaK97/BigDataSpark/blob/dev_branch/RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SCG_TopK.scala)
  - __[Q3]__ A modified version of SCG for identifying the Top-k dominating points of the skyline. <br>
    Navigate to [```RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SCG_Skyline_TopK.scala```](https://github.com/ChristinaK97/BigDataSpark/blob/dev_branch/RStarTreeScala/src/main/scala/TreeFunctions/Queries/PartialResults/SCG_Skyline_TopK.scala)


* __Distributed Computing with Apache Spark__<br> The proposed solution extends the algorithms to operate in a distributed computing environment using Apache Spark. This allows for parallel processing, enhancing the scalability of query processing in large-scale datasets.
    <br>Navigate to [```RStarTreeScala/src/main/scala/TreeFunctions/Queries/GlobalResults```](https://github.com/ChristinaK97/BigDataSpark/tree/dev_branch/RStarTreeScala/src/main/scala/TreeFunctions/Queries/GlobalResults)
    <br>An additional algorithm for distributed processing of the Q3 is also implemented in this package. 
    It is based on a modified version of the Batch Counting process used by SCG.  


* __Experiments__<br> Experimentation with diverse data distributions, examining the impact of data dimensionality and studying the speedup achieved by increasing parallelism.
  - Dataset generation: Navigate to the [```dist_generator```](https://github.com/ChristinaK97/BigDataSpark/tree/dev_branch/dist_generator) directory.
  - Execution results for aR*Tree: Navigate to [```RStarTreeScala/Results```](https://github.com/ChristinaK97/BigDataSpark/blob/dev_branch/RStarTreeScala/Results)


* __Baseline Solutions__<br> In addition to the proposed solution, a baseline approach is implemented for validation and comparative analysis.


### System Configuration

* __Scala version__: 2.13.12
* __Apache Spark version__: 3.5.0 (spark-3.5.0-bin-hadoop3-scala2.13)
* __sbt version__: 1.9.7
* __JDK__: 1.8

### Execution Parameters (aR*Tree solution)

To run the main program, you need to provide the following command line arguments:

- __Data Path (dataPath)__: The path to the input dataset.
    <br>Example: /path/to/dataset.csv
- __Number of Partitions / Executors (nPartitions)__:  The desired number of partitions or executors for distributed processing (1 partition per executor).
    <br>Example: 4
- __Top-k for Dataset (kForDataset)__: The value of k for the Top-k dominating points in the dataset (Q2).
    <br>Example: 10
- __Top-k for Skyline (kForSkyline)__: The value of k for the Top-k dominating points in the skyline (Q3).
    <br>Example: 5

Usage example:
> spark-submit rstartreescala_2.13-0.1.0-SNAPSHOT.jar /path/to/dataset.csv 4 10 5

