
# Установка Spark
wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz

tar -xvzf spark-3.5.5-bin-hadoop3.tgz

sudo mv spark-3.5.5-bin-hadoop3 /usr/local/spark


Добавляем в файл: ~/.bashrc

export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

Приеняем изменения:
source ~/.bashrc

Ставим пакеты:
pip3 install pyspark findspark