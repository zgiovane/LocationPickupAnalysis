# LocationPickupAnalysis

This README provides an overview and usage instructions for the Hotspot Analysis application developed using Scala and Apache Spark.

### Overview

The Hotspot Analysis application is designed to analyze large-scale geospatial datasets, particularly focusing on identifying hotspots within the data. It includes two main functionalities:

1. **Hot Zone Analysis**: This feature identifies hot zones within a given geographic area based on the density of points falling within specified rectangles.

2. **Hot Cell Analysis**: This functionality calculates the Getis-Ord statistic for each cell in a three-dimensional space, helping to identify statistically significant hot cells.

### Requirements
To run the Hotspot Analysis application, ensure that you have the following technologies installed:

- Apache Spark 3.4.1
- SparkSQL 3.4.1
- Scala 2.13
- Java 17.0.8
- Hadoop 3.3.6
- Python 3.10
- SBT 1.9.4

### Usage

To use the Hotspot Analysis application, follow these steps:

1. **Set up Apache Spark and Scala environment**:
   - Ensure that you have Apache Spark installed and configured on your system.
   - Install Scala and set up the necessary environment variables.

2. **Clone the repository**:
   ```
   git clone https://github.com/zgiovane/LocationPickupAnalysis.git
   ```

3. **Run the application**:
   - Modify the `Entrance.scala` file in the `cse512` package to specify your group name in the `appName` field.
   - Compile and run the application using the following command:
     ```
     spark-submit --class cse512.Entrance <path-to-jar-file> <output-path> <query-name> <query-parameters>
     ```
     - `<output-path>`: Specify the path where the output CSV file will be saved.
        > Example output path: test\output
     - `<query-name>`: Specify either `hotzoneanalysis` or `hotcellanalysis`.
     - `<query-parameters>`: Provide the necessary parameters for the chosen query.
        > Example query parameter: src\resources\point-hotzone.csv src\resources\zone-hotzone.csv 

4. **Interpret the results**:
   - The output CSV file will contain the results of the analysis.
   - For `hotzoneanalysis`, the output will include rectangles and the count of points within each rectangle.
   - For `hotcellanalysis`, the output will include the top 50 hottest cells based on the Getis-Ord statistic.
