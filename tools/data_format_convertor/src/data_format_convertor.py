import os.path
import shutil
import sys
import time
from os import path, walk
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
import argparse
import pathlib
import chardet
import logging

CSV_FORMAT = "csv"
SAS_FORMAT = "sas7bdat"
LOG_LEVEL = logging.INFO
LOG_FILE_PATH = "/tmp/format_convertor/app.log"
ERR_LOG_FILE_PATH = "/tmp/format_convertor/error.log"
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Set up logging configuration
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

# Create file handler
output_file_handler = logging.FileHandler(LOG_FILE_PATH)
output_file_handler.setLevel(LOG_LEVEL)
file_formatter = logging.Formatter(LOG_FORMAT)
output_file_handler.setFormatter(file_formatter)

# Create stream handler (console)
output_stream_handler = logging.StreamHandler()
output_stream_handler.setLevel(LOG_LEVEL)
stream_formatter = logging.Formatter(LOG_FORMAT)
output_stream_handler.setFormatter(stream_formatter)

# create error log handler
error_handler = logging.FileHandler(ERR_LOG_FILE_PATH)
error_handler.setFormatter(file_formatter)
error_handler.setLevel(logging.ERROR)

# Get the root logger and add the handlers
logger = logging.getLogger('')
logger.addHandler(output_file_handler)
logger.addHandler(output_stream_handler)
logger.addHandler(error_handler)

# Set up retry time and delay
MAX_RETRY = 3
RETRY_DELAY = 5  # seconds

PARTITION_SIZE = 256


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
        logger.warning("The given path is not a directory")
        sys.exit(1)
    totalFileNum = 0
    successFileNum = 0
    successFilesPath = []
    failedFileNum = 0
    failedFilesPath = []
    for root, dirs, files in walk(rootPath):
        for file in files:
            totalFileNum += 1
            inFilePathStr = path.join(root, file)
            if convertFileToParquet(spark, inFilePathStr, outDirPath, overwrite, **kwargs):
                successFileNum += 1
                successFilesPath.append(inFilePathStr)
            else:
                failedFileNum += 1
                failedFilesPath.append(inFilePathStr)
    logger.info(f"""
                   ################################Summery###############################
                   Total File number: {totalFileNum}
                   Success conversion file number: {successFileNum}
                   Success conversion file paths: {successFilesPath}
                   
                   Failed conversion file number: {failedFileNum}
                   Failed conversion file paths: {failedFilesPath}
                  """)


def convertFileToParquet(spark: SparkSession,
                         inFilePathStr: str,
                         outDirPath: str,
                         overwrite: bool = False,
                         **kwargs) -> bool:
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
                    if convertCsvToParquet(spark, inFilePathStr, outputPath, **kwargs):
                        return True
                elif fileExtension == f".{SAS_FORMAT}":
                    if 'partitionColumns' in kwargs:
                        if convertSasToParquet(spark, inFilePathStr, outputPath,
                                               partitionColumns=kwargs['partitionColumns']):
                            return True
                    else:
                        if convertSasToParquet(spark, inFilePathStr, outputPath):
                            return True
                else:
                    logger.warning(f"The given file format {fileExtension} is not supported yet. "
                                   f"Skip file {inFilePathStr}")
                    return False

            except Exception as e:
                logger.error(f"Failed to convert the file {inFilePathStr}: {e}")
            if retry < MAX_RETRY:
                logger.info(f"Retrying in  {RETRY_DELAY} seconds ...")
                time.sleep(RETRY_DELAY)
            else:
                logger.info("Max retries reached, stop ALL.")
                return False


def convertSasToParquet(spark: SparkSession, filePath: str, outPath: str,
                        partitionColumns: Optional[List[str]] = None, localFs: bool = True) -> bool:
    """
    This function read a sas file and convert it to parquet. If the given sas file can't be read by spark, an exception
    is raised
    :param localFs: specify spark read data from local file system
    :type localFs:
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
    logger.info(f"Start to convert sas file {filePath} to parquet file at {outPath}")
    if not checkFileFormat(filePath, SAS_FORMAT):
        logger.warning("Please enter a valid sas file path")
    if localFs:
        outPath = f"file://{outPath}"
        sparkInPath = f"file://{filePath}"

    try:
        df = spark.read \
            .format("com.github.saurfang.sas.spark") \
            .load(sparkInPath, forceLowercaseNames=True, inferLong=True)
        df.show(5)
        # determine the partition number by calculating the file size
        partitionNum = determinePartitionNumber(filePath)
        # repartition the dataframe with the given number
        df = df.repartition(partitionNum)
    except Exception as e:
        logger.error(f"Can't read the given path as a sas file with below exception {e}")
        raise ValueError(f"The given path is not in sas format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)

    else:
        df.write.mode("overwrite").parquet(outPath)

    # validate the output parquet file, if it is not valid, delete the parquet file
    if validateOutputParquetFile(spark, filePath, df, outPath):
        logger.info(f"Finish the sas file conversion with Success")
        return True
    else:
        logger.info(f"Finish the sas file conversion with error")
        return False


def determinePartitionNumber(filePath: str) -> int:
    """
    This function takes a file path, and calculate a partition number based on the given partition size
    It only works on local file system
    :param filePath:
    :type filePath:
    :return:
    :rtype:
    """
    # get the file size in bytes
    fileSize = os.path.getsize(filePath)
    # the partition size in bytes
    partitionSize = PARTITION_SIZE * 1024 * 1024
    partitionNum = int(fileSize / partitionSize)
    # if the partition number is too small, set it to 1
    if partitionNum <= 0:
        partitionNum = 1
    return partitionNum


def validateOutputParquetFile(spark: SparkSession, originFilePath: str,
                              originDf: DataFrame, parquetFilePath: str) -> bool:
    """
    This function validate the output parquet file with the origin file by using:
    - row number
    - column number
    - data schema
    :param originFilePath:
    :type originFilePath:
    :param spark:
    :type spark:
    :param originDf:
    :type originDf:
    :param parquetFilePath:
    :type parquetFilePath:
    :return:
    :rtype:
    """
    originRowNum = originDf.count()
    originColNum = len(originDf.columns)
    originSchema = originDf.schema

    newDf = spark.read.parquet(parquetFilePath)
    newRowNum = newDf.count()
    newColNum = len(originDf.columns)
    newSchema = newDf.schema
    if originRowNum != newRowNum:
        logger.error(f"The output parquet file of {originFilePath} has different row numbers")
        valid = False
    elif originColNum != newColNum:
        logger.error(f"The output parquet file of {originFilePath} has different column numbers")
        valid = False
    elif originSchema != newSchema:
        logger.error(f"The output parquet file of {originFilePath} has different schema")
        valid = False
    else:
        logger.info(f"The output parquet file is validated with the origin file {originFilePath}.\n"
                    f"Row number: {newRowNum}\n"
                    f"Column number: {newRowNum}\n"
                    f"Schema: {newSchema}")
        valid = True
    if not valid:
        try:
            shutil.rmtree(parquetFilePath)
            logger.info(f"The generated parquet file {parquetFilePath} is not valid, it has been deleted")
        except Exception as e:
            logger.error(f"The generated parquet file {parquetFilePath} is not valid. But unable to delete it: {e}")
    return valid


def convertCsvToParquet(spark: SparkSession, filePath: str, outPath: str, delimiter: str = ",", encoding: str = "utf-8",
                        partitionColumns: Optional[List[str]] = None) -> bool:
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
    logger.info(f"Start to convert csv file {filePath} to parquet file at {outPath}")
    if not checkFileFormat(filePath, CSV_FORMAT):
        logger.error("Please enter a valid csv file path")
        raise ValueError(f"The given path is not in csv format")
    outPath = f"file://{outPath}"
    try:
        filePath = f"file://{filePath}"
        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("delimiter", delimiter) \
            .option("encoding", encoding) \
            .csv(filePath)
        df.show(5)
    except Exception as e:
        logger.error(f"Can't read the given path as a csv file with below exception {e}")
        raise ValueError(f"The given path is not in csv format")
    if partitionColumns:
        df.write.partitionBy(partitionColumns).mode("overwrite").parquet(outPath)
    else:
        df.write.mode("overwrite").parquet(outPath)

    # validate the output parquet file, if it is not valid, delete the parquet file
    if validateOutputParquetFile(spark, filePath, df, outPath):
        logger.info(f"Finish the csv file conversion with success")
        return True
    else:
        logger.info(f"Finish the csv file conversion with error")
        return False


def checkFileFormat(filePath: str, expectedFormat: str) -> bool:
    """
    This function checks the file format with a given format
    :param filePath:
    :type filePath:
    :param expectedFormat:
    :type expectedFormat:
    :return:
    :rtype:
    """
    inFile = pathlib.Path(filePath)
    if not inFile.is_file():
        logger.warning(f"The given path {filePath} is not a file")
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
        logger.info(f"Inferred encoding value: {encoding}, with confidence: {confidence}")
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
    logger.info(f"User input delimiter value: '{delimiter}', encoding value: {encoding}, "
                f"partition columns : {partColumns}")
    # get dependent jar path
    rootLibPath = pathlib.Path.cwd().parent.parent.parent / "libs"
    parsoPath = rootLibPath / "parso-2.0.11.jar"
    sparkSasPath = rootLibPath / "spark-sas7bdat-3.0.0-s_2.12.jar"
    jarPathList = [str(parsoPath), str(sparkSasPath)]
    jarPath = ",".join(jarPathList)

    # build
    # the shuffle partition number should be the same as the executor number
    # The default max spark partition size is 128MB, which may result in too many small partition.
    # This lead to small task that slows down the performance.
    logger.info("Building spark session")
    try:
        spark = SparkSession.builder.master("local[*]") \
            .config("spark.driver.memory", "8g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.files.maxPartitionBytes", "256mb") \
            .config("spark.jars", jarPath) \
            .appName("DataFormatConvertor").getOrCreate()
        logger.info("The spark session is created successfully")
    except Exception as e:
        logger.error(f"Failed to create spark session: {e}\n "
                     f"Stop all")
        sys.exit(1)
    logger.info("Start file conversion")
    # if input is a file call function with convertFileToParquet
    if path.isfile(inputPath):
        if partColumns:
            convertFileToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                 delimiter=delimiter, encoding=encoding,
                                 partitionColumns=partColumns)
        else:
            convertFileToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                 delimiter=delimiter, encoding=encoding)
    # if the input is a directory
    else:
        if partColumns:
            convertFilesToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                  delimiter=delimiter, encoding=encoding,
                                  partitionColumns=partColumns)
        else:
            convertFilesToParquet(spark, inputPath, outputPath, overwrite=args.overwrite,
                                  delimiter=delimiter, encoding=encoding)
    logger.info("End file conversion")


if __name__ == "__main__":
    main()
