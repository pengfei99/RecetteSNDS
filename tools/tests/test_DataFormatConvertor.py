import pathlib

import pytest
from pyspark.sql.session import SparkSession
from tools.src.data_format_convertor import convertSasToParquet, checkCSVEncoding, convertFilesToParquet, \
    convertCsvToParquet, convertFileToParquet
import subprocess


@pytest.fixture
def sparkSession():
    # get dependent jar path
    rootLibPath = pathlib.Path.cwd().parent.parent / "libs"
    parsoPath = rootLibPath / "parso-2.0.11.jar"
    sparkSasPath = rootLibPath / "spark-sas7bdat-3.0.0-s_2.12.jar"
    jarPathList = [str(parsoPath), str(sparkSasPath)]
    jarPath = ",".join(jarPathList)

    # build
    #
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.files.maxPartitionBytes", "256mb") \
        .config("spark.jars", jarPath) \
        .appName("DataFormatConvertor").getOrCreate()

    return spark


def test_convertFileToParquet_withSas(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/airline.sas7bdat"
    outputPath = "/tmp"
    convertFileToParquet(sparkSession, inputFile, outputPath)


def test_convertFileToParquet_withPartitionColumn(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/airline.sas7bdat"
    outputPath = "/tmp"
    partColumns = ["year"]
    convertFileToParquet(sparkSession, inputFile, outputPath, partitionColumns=partColumns)


def test_convertFileToParquet_withCsv(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/bad_encoding.csv"
    outputPath = "/tmp"
    convertFileToParquet(sparkSession, inputFile, outputPath, delimiter=";", encoding="windows-1252")


def test_convertFilesToParquet(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data"
    outputPath = "/tmp/generated_parquet_files"
    convertFilesToParquet(sparkSession, inputFile, outputPath)


def test_convertSasToParquet(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/airline.sas7bdat"
    outputPath = "/tmp/airline"
    convertSasToParquet(sparkSession, inputFile, outputPath)


def test_convertCsvToParquet_withDelimiterEncoding(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/bad_encoding.csv"
    outputPath = "/tmp/bad_encoding"
    convertCsvToParquet(sparkSession, inputFile, outputPath, delimiter=";", encoding="windows-1252")


def test_convertCsvToParquet_withPartition(sparkSession):
    inputFile = "/home/pengfei/git/RecetteSNDS/data/bad_encoding.csv"
    outputPath = "/tmp/bad_encoding"
    partColumns = ["Taille"]
    convertCsvToParquet(sparkSession, inputFile, outputPath, delimiter=";", encoding="windows-1252",
                        partitionColumns=partColumns)


def test_checkCSVEncoding():
    inputFile = "/home/pengfei/git/RecetteSNDS/data/bad_encoding.csv"
    checkCSVEncoding(filePath=inputFile)


def test_runWithArgparse_withFolderMode():
    # python data_format_convertor.py /home/pengfei/git/RecetteSNDS/data /tmp/data --delimiter ";"
    # --encoding windows-1252 --partitionColumns Type,Taille
    appPath = "/home/pengfei/git/RecetteSNDS/tools/src/data_format_convertor.py"
    inputPath = "/home/pengfei/git/RecetteSNDS/data"
    outputPath = "/tmp/data"
    command = f'python {appPath} {inputPath} {outputPath} --delimiter ";" --encoding windows-1252'
    subprocess.run(command, shell=True)
