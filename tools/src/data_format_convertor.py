import shutil
import sys
from os import path, walk
from typing import Optional, List

from pyspark.sql import SparkSession
import argparse
import pathlib
import chardet

CSV_FORMAT = "csv"
SAS_FORMAT = "sas7bdat"


def convertFilesToParquet(spark: SparkSession, rootPath: str, outDirPath: str, delimiter: str = ",",
                          encoding: str = "utf-8",
                          partitionColumns: Optional[List[str]] = None):
    """
    This function takes an input folder, scans all the file in it, if its extension is csv or sas, it will be converted
    to parquet

    :param delimiter:
    :type delimiter:
    :param partitionColumns:
    :type partitionColumns:
    :param encoding:
    :type encoding:
    :param spark:
    :type spark:
    :param rootPath:
    :type rootPath:
    :param outDirPath:
    :type outDirPath:
    :return:
    :rtype:
    """
    if not path.isdir(rootPath):
        print("The given path is not a directory")
        sys.exit(1)

    for root, dirs, files in walk(rootPath):
        for file in files:
            inFilePathStr = path.join(root, file)
            convertFileToParquet(spark, inFilePathStr, outDirPath)


def convertFileToParquet(spark: SparkSession, inFilePathStr: str, outDirPath: str, **kwargs):
    inFilePath = pathlib.Path(inFilePathStr)
    outputPath = path.join(outDirPath, str(inFilePath.name.split(".", 1)[0]))
    fileExtension = str(inFilePath.suffix).lower()
    if fileExtension == f".{CSV_FORMAT}":
        convertCsvToParquet(spark, inFilePathStr, outputPath, **kwargs)

    elif fileExtension == f".{SAS_FORMAT}":
        if 'partitionColumns' in kwargs:
            convertSasToParquet(spark, inFilePathStr, outputPath, partitionColumns=kwargs['partitionColumns'])
        else:
            convertSasToParquet(spark, inFilePathStr, outputPath)
    else:
        raise ValueError(f"The given file format {fileExtension} is not supported yet")


def convertSasToParquet(spark: SparkSession, filePath: str, outPath: str,
                        partitionColumns: Optional[List[str]] = None) -> None:
    """
    This function read a sas file and convert it to parquet. If the given sas file can't be read by spark, an exception
    is raised
    :param partitionColumns:
    :type partitionColumns:
    :param spark:
    :type spark:
    :param filePath:
    :type filePath:
    :param outPath:
    :type outPath:
    :return:
    :rtype:
    """
    if not checkFileFormat(filePath, "sas"):
        print("Please enter a valid sas file path")
    try:
        df = spark.read \
            .format("com.github.saurfang.sas.spark") \
            .load(filePath, forceLowercaseNames=True, inferLong=True)
        df.show(5)
    except Exception as e:
        print(f"Can't read the given path as a sas file with below exception {e}")
        raise ValueError(f"The given path is not in sas format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)
    else:
        df.write.mode("overwrite").parquet(outPath)


def convertCsvToParquet(spark: SparkSession, filePath: str, outPath: str, delimiter: str = ",", encoding: str = "utf-8",
                        partitionColumns: Optional[List[str]] = None) -> None:
    """
    This function read a csv file (not partitioned), then convert it to parquet
    :param partitionColumns: The list of column names which we want to partition the data
    :type partitionColumns: Optional[List[str]]
    :param spark: A Spark session
    :type spark: SparkSession
    :param filePath: input csv file path
    :type filePath: str
    :param outPath: outPath of the generated parquet files
    :type outPath: str
    :param delimiter: the delimiter of the csv file, the default value is ,
    :type delimiter: str
    :param encoding: the encoding code of the csv file, the default value is utf-8
    :type encoding: str
    :return: None
    :rtype:
    """

    if checkFileFormat(filePath, "csv"):
        print("Please enter a valid csv file path")
        raise ValueError(f"The given path is not in csv format")

    try:
        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("delimiter", delimiter) \
            .option("encoding", encoding) \
            .csv(filePath)
        df.show(5)
    except Exception as e:
        print(f"Can't read the given path as a csv file with below exception {e}")
        raise ValueError(f"The given path is not in csv format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)
    else:
        df.write.mode("overwrite").parquet(outPath)


def checkFileFormat(filePath: str, expectedFormat: str):
    inFile = pathlib.Path(filePath)
    if not inFile.is_file():
        print(f"The given path {filePath} is not a file")
        return False

    if expectedFormat.lower() == CSV_FORMAT:
        if inFile.suffix.lower() == f".{CSV_FORMAT}":
            return True
        else:
            return False
    elif expectedFormat.lower() == SAS_FORMAT:
        if inFile.suffix.lower() == f".{SAS_FORMAT}":
            return True
        else:
            return False


def checkCSVEncoding(filePath: str):
    with open(filePath, "rb") as file:
        rawData = file.read()
        result = chardet.detect(rawData)
        encoding = result['encoding']
        confidence = result['confidence']
        print(type(confidence))
        print(f"Inferred encoding value: {encoding}, with confidence: {confidence}")
        if confidence > 0.6:
            return encoding
        else:
            print("Confidence value is too low, we can't determine the encoding")
            return None


def main():
    parser = argparse.ArgumentParser(description="This CLI can help you to convert sas or csv files to parquet format")
    parser.add_argument("inputDirPath", help="The root directory path which contains the csv or sas files")
    parser.add_argument("outputDirPath", help="The root directory path which contains the output parquet files")
    parser.add_argument("--isFile", help="By default this option is set to False, If this option is enabled "
                                         "by putting True, the application takes one single file path as input")
    parser.add_argument("--delimiter", "If the input csv files does not use the default delimiter `,`, user need to"
                                       "provide the delimiter value")
    parser.add_argument("--encoding", "if the input csv file does not use the default encoding value `utf-8`, user "
                                      "need to provide the encoding value")

    if len(sys.argv) != 3:
        print("You must enter a directory path which indicates the input data")
        sys.exit(1)

    # get dependent jar path
    rootLibPath = pathlib.Path.cwd().parent.parent / "libs"
    parsoPath = rootLibPath / "parso-2.0.11.jar"
    sparkSasPath = rootLibPath / "spark-sas7bdat-3.0.0-s_2.12.jar"
    jarPathList = [str(parsoPath), str(sparkSasPath)]
    jarPath = ",".join(jarPathList)

    # build
    # the shuffle partition number should be the same as the executor number
    # The default max spark partition size is 128MB, which may result in too many small partition.
    # This lead to small task that slows down the performance.
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.files.maxPartitionBytes", "256mb") \
        .config("spark.jars", jarPath) \
        .appName("DataFormatConvertor").getOrCreate()


if __name__ == "__main__":
    main()
