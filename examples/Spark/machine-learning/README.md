#Extra python packages

Need to install the following packages:

 pip install pandas
 pip install matplotlib
 pip install seaborn
 pip install pyspark
 pip install findspark


Google colab

 import pandas as pd
 import io
 from google.colab import files
 uploaded = files.upload()

 df = pd.read_csv(io.BytesIO(uploaded['FileName.csv']))

 check

 df.head()

