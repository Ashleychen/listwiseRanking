chmod 777 convert_pairwise_text_data_to_bin
chmod 777 convert_classify_text_data_to_bin
datafile=`basename $map_input_file`.$mapred_task_id
datafile=${datafile%_*}
echo "[INFO] generating data into file $datafile ..."
if [ $# -eq 3 ]; then
    if [ $2 -eq 1 ]; then
        ./convert_pairwise_text_data_to_bin $3 $datafile
        if [ $? -ne 0 ]; then
            echo "[ERROR] fail to generate data"
            exit 1
        fi
    else
        ./convert_classify_text_data_to_bin $3 $datafile
        if [ $? -ne 0 ]; then
            echo "[ERROR] fail to generate data"
            exit 1
        fi
    fi
else
    ./convert_pairwise_text_data_to_bin $3 $datafile
    if [ $? -ne 0 ]; then
        echo "[ERROR] fail to generate data"
        exit 1
    fi
fi
echo "[INFO] finish writing data file $datafile" 

echo "[INFO] copying data file to hdfs $1/ ..." 
${HADOOP_HOME}/bin/hadoop --config ./hadoop_conf/conf fs -test -e $1/$datafile
if [ $? -ne 0 ]; then
    ${HADOOP_HOME}/bin/hadoop --config ./hadoop_conf/conf fs -put $datafile $1/
fi

if [ $? -ne 0 ]; then
    echo "[ERROR] fail to copy data file"
    exit 1
fi
echo "[INFO] finish copying data file" 
