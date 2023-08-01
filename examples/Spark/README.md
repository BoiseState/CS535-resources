#Extra python packages

Need to install the following packages:

 pip install pandas
 pip install matplotlib
 pip install seaborn
 pip install pyspark
 pip install findspark

#Using Google colabs

!pip install findspark
!pip install pyspark

from google.colab import files
uploaded = files.upload()
--> will display file chooser
the read file as usual in spark



#Reading local/drive files in Gooogle colab

Reading local files into Google colab

 import pandas as pd
 import io
 from google.colab import files
 uploaded = files.upload()

 df = pd.read_csv(io.BytesIO(uploaded['FileName.csv']))

 check

 df.head()


Reading files from GitHub

url = 'copied_raw_github_link'
df = pd.read_csv(url)


Reading files from Google drive

from google.colab import drive
drive.mount('content/drive')
path_to_data = '/content/drive/My Drive/Data'
