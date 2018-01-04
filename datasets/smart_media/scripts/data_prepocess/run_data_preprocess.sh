#!/bin/bash
source ~/.bashrc

alias hadoop_bin=hadoop_yq_heng
python_bin="python-epd/bin/python"
archive_path="/app/ssg/nlp/ol/fangxiaomin01/archives/python-epd.tar.gz#python-epd"
lego_path="${feedgr_path}/archives/lego_lib.tar.gz#lego_lib"
hadoop_conf_path="${feedgr_path}/archives/hadoop_conf.tgz#hadoop_conf"
date_range="one_week"
root_path="/home/chenyaxue/chenyaxue/ranking/datasets/smart_media"
script_file="${root_path}/scripts/data_prepocess/data_preprocess.py"
local_features_map="${root_path}/tmp_data/features_map_${date_range}"
raw_data_path="/app/ssg/nlp/ol/fangxiaomin01/ranking/addressa/${date_range}"
features_map_path="/app/ssg/nlp/ol/fangxiaomin01/ranking/addressa/features_map_${date_range}"
example_path="/app/ssg/nlp/ol/fangxiaomin01/ranking/addressa/example_${date_range}"
binary_path="/app/ssg/nlp/ol/fangxiaomin01/ranking/addressa/binary_${date_range}"
binary_log_path="/app/ssg/nlp/ol/fangxiaomin01/ranking/addressa/binary_log_${date_range}"

# get features map
#hadoop_bin fs -rmr ${features_map_path}
#hadoop_bin streaming \
#    -input ${raw_data_path} \
#    -output ${features_map_path} \
#    -mapper "${python_bin} data_preprocess.py --task parse_json" \
#    -reducer "${python_bin} data_preprocess.py --task merge_features" \
#    -jobconf mapred.job.name="nlp_chenyaxue_get_features_map_${date_range}" \
#    -jobconf mapred.job.priority=NORMAL\
#    -jobconf mapred.job.map.capacity=1010 \
#    -jobconf mapred.job.reduce.capacity=1000 \
#    -jobconf mapred.reduce.tasks=1 \
#    -jobconf mapred.map.memory.limit=800 \
#    -jobconf mapred.map.over.capacity.allowed=false \
#    -jobconf mapred.job.queue.name=nlp-data-mining \
#    -cacheArchive ${archive_path} \
#    -file ${script_file}
#
#rm -rf ${local_features_map}
#hadoop_bin fs -cat ${features_map_path}/* > ${local_features_map}

# convert examples
hadoop_bin fs -rmr ${example_path}
hadoop_bin streaming \
    -input ${raw_data_path} \
    -output ${example_path} \
    -mapper "${python_bin} data_preprocess.py --task convert_examples --features_map features_map_${date_range}" \
    -jobconf mapred.job.name="nlp_chenyaxue_convert_example_${date_range}" \
    -jobconf mapred.job.priority=NORMAL\
    -jobconf mapred.job.map.capacity=1010 \
    -jobconf mapred.job.reduce.capacity=1000 \
    -jobconf mapred.reduce.tasks=0 \
    -jobconf mapred.map.memory.limit=800 \
    -jobconf mapred.map.over.capacity.allowed=false \
    -jobconf mapred.job.queue.name=nlp-data-mining \
    -cacheArchive ${archive_path} \
    -file ${script_file} \
    -file ${local_features_map}

# generate_schema
# convert examples to binary
hadoop_bin fs -rmr ${binary_path}
hadoop_bin fs -rmr ${binary_log_path}
hadoop_bin fs -mkdir ${binary_path}
hadoop_bin streaming \
    -input ${example_path} \
    -output ${binary_log_path} \
    -mapper "export LD_LIBRARY_PATH=./lego_lib/lib; sh dump_binary_train_lego.sh ${binary_path} false schema_${date_range}" \
    -jobconf mapred.job.name="nlp_chenyaxue_convert_example_to_${date_range}" \
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
    -file ./conf/schema_${date_range}
