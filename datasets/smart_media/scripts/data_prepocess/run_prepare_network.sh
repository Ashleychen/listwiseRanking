#!/bin/bash
source ~/.bashrc
source ./conf.sh

# generate data feed
function generate_data_feed() {
    if [ "${model_type}" == "rnn" ]; then
        python network_generater.py \
            --task data_feed \
            --features_map ${local_features_map} \
            --data_feed_file ${local_data_feed_file} \
            --is_rnn
    elif [ "${model_type}" == "dnn" ]; then
        python network_generater.py \
            --task data_feed \
            --features_map ${local_features_map} \
            --data_feed_file ${local_data_feed_file}
    fi
}

function prepare_network() {
    if [ ${prepare_network_choice} -eq 1 ]; then
        generate_data_feed
    fi
}

prepare_network
