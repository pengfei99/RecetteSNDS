import os.path
import shutil
import sys
import time
from os import path, walk
from typing import Optional, List

from pyspark.sql import SparkSession
import argparse
import pathlib
import chardet
import logging

CSV_FORMAT = "csv"
SAS_FORMAT = "sas7bdat"
LOG_LEVEL = logging.INFO
LOG_FILE_PATH = "/tmp/format_convertor/app.log"
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Set up logging configuration
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

# Create file handler
file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setLevel(LOG_LEVEL)
file_formatter = logging.Formatter(LOG_FORMAT)
file_handler.setFormatter(file_formatter)

# Create stream handler (console)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(LOG_LEVEL)
stream_formatter = logging.Formatter(LOG_FORMAT)
stream_handler.setFormatter(stream_formatter)

# Get the root logger and add the handlers
logger = logging.getLogger('')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Set up retry time and delay
MAX_RETRY = 3
RETRY_DELAY = 5  # seconds


def convertFilesToParquet(spark: SparkSession, rootPath: str, outDirPath: str, overwrite: bool = False, **kwargs):
    """
    This function takes an input folder, scans all the file in it, if its extension is csv or sas, it will be converted
    to parquet. It can take a list of implicite argument such as delimiter, encoding, partitionColumns.

    :param overwrite:
    :type overwrite:
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
        logging.warning("The given path is not a directory")
        sys.exit(1)

    for root, dirs, files in walk(rootPath):
        for file in files:
            inFilePathStr = path.join(root, file)
            convertFileToParquet(spark, inFilePathStr, outDirPath, overwrite, **kwargs)


def convertFileToParquet(spark: SparkSession, inFilePathStr: str, outDirPath: str, overwrite: bool = False, **kwargs):
    inFilePath = pathlib.Path(inFilePathStr)
    outputPath = path.join(outDirPath, str(inFilePath.name.split(".", 1)[0]))
    if os.path.exists(outDirPath) and not overwrite:
        logger.info(f"The target out put parquet file {outDirPath} already exists. Skip the conversion of "
                    f"file {inFilePathStr}. If you want to overwrite the existing parquet file, please enable "
                    "the overwrite option")
    else:
        fileExtension = str(inFilePath.suffix).lower()
        for retry in range(MAX_RETRY + 1):
            try:
                if fileExtension == f".{CSV_FORMAT}":
                    convertCsvToParquet(spark, inFilePathStr, outputPath, **kwargs)
                elif fileExtension == f".{SAS_FORMAT}":
                    if 'partitionColumns' in kwargs:
                        convertSasToParquet(spark, inFilePathStr, outputPath,
                                            partitionColumns=kwargs['partitionColumns'])
                    else:
                        convertSasToParquet(spark, inFilePathStr, outputPath)
                else:
                    logger.warning(f"The given file format {fileExtension} is not supported yet. "
                                   f"Skip file {inFilePathStr}")
                break
            except Exception as e:
                logger.error(f"Failed to convert the file {inFilePathStr}: {e}")
            if retry < MAX_RETRY:
                logger.info(f"Retrying in  {RETRY_DELAY} seconds ...")
                time.sleep(RETRY_DELAY)
            else:
                logger.info("Max retries reached, stop ALL.")


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
    logging.info(f"Start to convert sas file {filePath} to parquet file at {outPath}")
    if not checkFileFormat(filePath, SAS_FORMAT):
        logging.warning("Please enter a valid sas file path")
    try:
        df = spark.read \
            .format("com.github.saurfang.sas.spark") \
            .load(filePath, forceLowercaseNames=True, inferLong=True)
        df.show(5)
    except Exception as e:
        logging.error(f"Can't read the given path as a sas file with below exception {e}")
        raise ValueError(f"The given path is not in sas format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)
    else:
        df.write.mode("overwrite").parquet(outPath)
    logging.info(f"Finish the sas file conversion")


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
    logging.info(f"Start to convert csv file {filePath} to parquet file at {outPath}")
    if not checkFileFormat(filePath, CSV_FORMAT):
        logging.error("Please enter a valid csv file path")
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
        logging.error(f"Can't read the given path as a csv file with below exception {e}")
        raise ValueError(f"The given path is not in csv format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)
    else:
        df.write.mode("overwrite").parquet(outPath)
    logging.info(f"Finish the csv file conversion")


def checkFileFormat(filePath: str, expectedFormat: str):
    inFile = pathlib.Path(filePath)
    if not inFile.is_file():
        logging.warning(f"The given path {filePath} is not a file")
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
        logging.info(f"Inferred encoding value: {encoding}, with confidence: {confidence}")
        if confidence > 0.6:
            return encoding
        else:
            print("Confidence value is too low, we can't determine the encoding")
            return None


def main():
    # define the argparser arguments
    parser = argparse.ArgumentParser(description="This CLI can help you to convert sas or csv files to parquet format")
    parser.add_argument("inputPath", help="The root directory path which contains the csv or sas files, it can be"
                                          " a folder or a single file path  ")
    parser.add_argument("outputPath", help="The root directory path which contains the output parquet files")
    # parser.add_argument("--isFile", help="By default this option is set to False, If this option is enabled "
    #                                      "by putting True, the application takes one single file path as input")
    parser.add_argument("--delimiter", help="If the input csv files does not use the default delimiter ',', user need "
                                            "to provide the delimiter value. Important note, as python argparse "
                                            "considers `;` as separator also, you need to add quotes to semicolon")
    parser.add_argument("--encoding", help="if the input csv file does not use the default encoding value utf-8, user "
                                           "need to provide the encoding value")
    parser.add_argument("--partitionColumns", help="If no partition columns is provided, use the default hash "
                                                   "partition of the spark. If a column name is given, then use "
                                                   "the range partition. Note if the given column does not exist "
                                                   "in the target file, an error will be raised")
    parser.add_argument("--overwrite", action="store_true", help="Enable overwrite mode")
    # parse the arguments
    args = parser.parse_args()
    inputPath = args.inputPath
    outputPath = args.outputPath
    # isFile = args.isFile == "True" or "False"
    delimiter = args.delimiter or ","
    encoding = args.encoding or "utf-8"
    # note if the optional arg is empty, a None will be returned
    if args.partitionColumns:
        partColumns = str(args.partitionColumns).split(",")
    else:
        partColumns = None
    logging.info(f"User input delimiter value: '{delimiter}', encoding value: {encoding}, "
                 f"partition columns : {partColumns}")
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
    logging.info("Building spark session")
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.files.maxPartitionBytes", "256mb") \
        .config("spark.jars", jarPath) \
        .appName("DataFormatConvertor").getOrCreate()
    logging.info("Start file conversion")
    if path.isfile(inputPath):
        if partColumns:
            convertFileToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                 delimiter=delimiter, encoding=encoding,
                                 partitionColumns=partColumns)
        else:
            convertFileToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                 delimiter=delimiter, encoding=encoding)
    else:
        if partColumns:
            convertFilesToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                  delimiter=delimiter, encoding=encoding,
                                  partitionColumns=partColumns)
        else:
            convertFilesToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                  delimiter=delimiter, encoding=encoding)
    logging.info("End file conversion")


if __name__ == "__main__":
    main()
