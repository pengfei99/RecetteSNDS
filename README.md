# RecetteSNDS

This repo will group all the tools (e.g. Data engineering, Data analytics, Data visualization, etc.) that we develop
for the project managing SNDS data.

# Data format convertor

First tools is the data convertor, you can find the code source in [data_format_convertor.py](tools/data_format_convertor/src/data_format_convertor.py)
The dependencies in [requirements.txt](tools/data_format_convertor/requirements.txt)

```bash
# To use it, you need to first install the dependencies 
pip install -r requirements.txt

# below is an example on how to run the tool
python data_format_convertor.py /input/data /output/data --delimiter ";" --encoding windows-1252 --partitionColumns Type,Taille

# for help on the argument 
python data_format_convertor.py -h
```
