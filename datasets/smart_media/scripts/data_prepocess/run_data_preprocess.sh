#!/bin/bash
source ~/.bashrc
source ./conf.sh

# get features map
function get_features_map() {
    hadoop_bin fs -rmr ${features_map_path}
    hadoop_bin streaming \
        -input ${raw_data_path} \
        -output ${features_map_path} \
        -mapper "${python_bin} data_preprocess.py --task 1" \
        -reducer "${python_bin} data_preprocess.py --task 2" \
        -jobconf mapred.job.name="nlp_chenyaxue_get_features_map_${date_range}" \
        -jobconf mapred.job.priority=NORMAL\
        -jobconf mapred.job.map.capacity=1010 \
        -jobconf mapred.job.reduce.capacity=1000 \
        -jobconf mapred.reduce.tasks=1 \
        -jobconf mapred.map.memory.limit=800 \
        -jobconf mapred.map.over.capacity.allowed=false \
        -jobconf mapred.job.queue.name=nlp-data-mining \
        -cacheArchive ${archive_path} \
        -file ${script_file}

    rm -rf ${local_features_map}
    hadoop_bin fs -cat ${features_map_path}/* > ${local_features_map}
}

# convert examples
function convert_examples() {
    task=3
    if [ "${model_type}" == "rnn" ]; then
        task=4
    fi
    hadoop_bin fs -rmr ${example_path}
    hadoop_bin streaming \
        -input ${raw_data_path} \
        -output ${example_path} \
        -mapper "${python_bin} data_preprocess.py --task ${task} --features_map features_map_${model_type}_${date_range}" \
        -jobconf mapred.job.name="nlp_chenyaxue_convert_example_${model_type}_${date_range}" \
        -jobconf mapred.job.priority=NORMAL\
        -jobconf mapred.job.map.capacity=1010 \
        -jobconf mapred.job.reduce.capacity=1000 \
        -jobconf mapred.reduce.tasks=0 \
        -jobconf mapred.map.memory.limit=1600 \
        -jobconf mapred.map.over.capacity.allowed=false \
        -jobconf mapred.job.queue.name=nlp-data-mining \
        -cacheArchive ${archive_path} \
        -file ${script_file} \
        -file ${local_features_map}
}

# generate schema
function generate_schema() {
    if [ "${model_type}" == "rnn" ]; then
        python network_generater.py \
            --task schema \
            --features_map ${local_features_map} \
            --schema_file ${local_schema_file} \
            --is_rnn
    elif [ "${model_type}" == "dnn" ]; then
        python network_generater.py \
            --task schema \
            --features_map ${local_features_map}
    fi
}

# convert examples to binary
function convert_examples_to_binary() {
    hadoop_bin fs -rmr ${binary_path}
    hadoop_bin fs -rmr ${binary_log_path}
    hadoop_bin fs -mkdir ${binary_path}
    hadoop_bin streaming \
        -input ${example_path} \
        -output ${binary_log_path} \
        -mapper "export LD_LIBRARY_PATH=./lego_lib/lib; sh dump_binary_train_lego.sh ${binary_path} false schema_${model_type}_${date_range}" \
        -jobconf mapred.job.name="nlp_chenyaxue_convert_example_to_${model_type}_${date_range}" \
        -jobconf mapred.job.priority=NORMAL\
        -jobconf mapred.job.map.capacity=1010 \
        -jobconf mapred.job.reduce.capacity=1000 \
        -jobconf mapred.reduce.tasks=10 \
        -jobconf mapred.map.memory.limit=800 \
        -jobconf mapred.reduce.memory.limit=1600 \
        -jobconf mapred.map.over.capacity.allowed=false \
        -jobconf mapred.job.queue.name=nlp-data-mining \
        -cacheArchive ${lego_path} \
        -cacheArchive ${hadoop_conf_path} \
        -file ./dump_binary_train_lego.sh \
        -file ./convert_pairwise_text_data_to_bin \
        -file ./convert_classify_text_data_to_bin \
        -file ${local_schema_file}
}

function data_preprocess() {
    if [ ${data_preprocess_choice} -eq 1 ]; then
        get_features_map
    elif [ ${data_preprocess_choice} -eq 2 ]; then
        convert_examples
    elif [ ${data_preprocess_choice} -eq 3 ]; then
        generate_schema
    elif [ ${data_preprocess_choice} -eq 4 ]; then
        convert_examples_to_binary
    fi
}

data_preprocess
