import argparse
import json
import logging
import string
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Feature:
    def __init__(self, field_num):
        self.clear_feature()

    def set_example_id(self, example_id):
        self.id = example_id

    def add_features(self, field_idx, feature_ids, is_need_uniq):
        for feature_id in feature_ids:
            if is_need_uniq and feature_id in self.feature_slots[field_idx]:
                continue
            self.feature_slots[field_idx].append(feature_id)

    def add_label(self, label):
        self.labels.append(label)

    def clear_feature(self, field_num):
        self.feature_slots = []
        self.labels = []
        for i in xrange(field_num):
            self.feature_slots.append([])

    def to_example(self):
        example_str = str(self.id) + ';'
        for i in xrange(len(self.feature_slots)):
            example_str += ' '.join(map(str, self.feature_slots[i]))
            example_str += ';'
        example_str += ' '.join(map(str, self.labels))
        print example_str

class ListFeature(Feature):
    def __init__(self, field_num, variable_len_fields):
        Feature.__init__(self, field_num)
        self.feature_bounds = dict()
        for field_idx in xrange(field_num):
            if field_idx in variable_len_fields:
                self.feature_bounds[field_idx] = [0]

    def generate_list_feature(self, feature_list, variable_len_fields):
        field_num = len(feature_list[0].feature_slots)
        for field_idx in xrange(field_num):
            for item_idx in xrange(len(feature_list)):
                if len(feature_list[item_idx].feature_slots[field_idx]) == 0 \
                        and field_idx not in variable_len_fields:
                    self.feature_slots[field_idx].append([0])
                else:
                    self.feature_slots[field_idx].append(
                            feature_list[item_idx].feature_slots[field_idx])
                if field_idx in feature_bounds:
                    self.feature_bounds[field_idx].append(
                            feature_bounds[field_idx - 1] + len(
                                feature_list[item_idx].feature_slots[field_idx]))
        label_size = len(feature_list[0].labels)
        for label_idx in xrange(label_size):
            label_list = []
            for item_idx in xrange(len(feature_list)):
                label_list.append(feature_list[item_idx][label_idx]
            self.labels.append(label_list)

    def to_example(self):
        example_str = str(self.id) + ';'
        for i in xrange(len(self.feature_slots)):
            example_str += ' '.join([' '.join(map(str, item_feature_list
                )) for item_feature_list in self.feature_slots[i]])
            if i in self.feature_bounds:
                example_str += ';'
                example_str += ' '.join(map(str, self.feature_bounds[i][1:]))
        for i in xrange(len(self.labels)):
            example_str += ';'
            ' '.join(map(str, self.labels[i]))
        print example_str

class DataParser:

    def __init__(self, features_map_file, variable_len_fields_file,
        valid_fields_file, default_value_file, flag):
        self.flag = flag
        self.field_idx_map, self.feature_id_map = load_features_map(
                features_map_file)
        self.load_valid_fields(valid_fields_file)
        self.load_default_value_map(default_value_file)
        if flag == 'listwise':
            self.variable_len_fields = load_variable_len_fields(
                    variable_len_fields_file)

    def generate_feature(self)
        if flag == 'pointwise':
            fea = Feature(len(self.field_idx_map))
        elif flag == 'listwise':
            fea = ListFeature(len(self.field_idx_map), self.variable_len_fields)
        return fea

    def load_features_map(self, features_map_file):
        field_idx_map = dict()
        feature_id_maps = []
        f = open(features_map_file)
        field_count, feature_count = map(int, f.readline().strip().split('\x01'))
        for field_id in xrange(field_count):
            tokens = f.readline().strip().split('\x01')
            field_name = tokens[0]
            cur_feature_count = int(tokens[1])
            field_idx_map[field_name] = field_id
            feature_id_maps.append(dict())
            for i in xrange(cur_feature_count):
                tokens = f.readline().rstrip().split('\x01')
                feature_name = tokens[0]
                feature_id = int(tokens[1])
                feature_id_maps[field_id][feature_name] = feature_id
        return field_idx_map, feature_id_map

    def load_valid_fields(self, valid_fields_file):
        self.valid_fields = set()
        f = open(valid_fields_file)
        line = f.readline().strip()
        valid_fields_size = int(line)
        for i in xrange(valid_fields_size):
            line = f.readline().strip()
            self.valid_fields.add(line)

    def load_variable_len_fields(self, variable_len_fields_file):
        variable_len_fiels = set()
        f = open(variable_len_fields_file)
        line = f.readline().strip()
        variable_len_fields_size = int(line)
        for i in xrange(variable_len_fields_size):
            line = f.readline().strip()
            field_idx = self.field_idx_map[line]
            variable_len_fields.add(field_idx)
        return variable_len_fields

    def load_default_value_map(self, default_value_file):
        self.default_value_map = dict()
        f = open(default_value_file)
        for line in f:
            key, val = line.strip().split('\x01')
            self.default_value_map[key] = float(val)

    def update_feature(fea, field_name, feature_names, is_need_uniq):
        field_idx = self.field_idx_map[field_name]
        feature_ids = []
        for feature_name in feature_names:
            if not isinstance(feature_name, unicode):
                feature_name = str(feature_name)
            feature_id = self.feature_id_map[field_idx][feature_name.encode(
                'utf-8')]
            feature_ids.append(feature_id)
        fea.add_features(field_idx, feature_ids, is_need_uniq)
        return fea

    def extract_feature(self, json_dict):
        fea = self.generate_feature()
        for key in json_dict.keys():
            key = key.replace('-', '_')
            if isinstance(json_dict[key], list):
                if 'author' == key:
                    fea = self.update_features(fea, key, json_dict[key], True)
                if 'profile' == key:
                    item_list = json_dict[key]
                    for item_json in item_list:
                        feature = item_json['item']
                        field_list = item_json['groups']
                        for field_json in field_list:
                            occurrence = field_json['count']
                            field = field_json['group'].replace('-', '_')
                            weight = field_json['weight']
                            if 'author' == field:
                                fea = self.update_features(fea, field, [feature],
                                        True)
                            else:
                                fea = self.update_features(fea, field, [feature],
                                        False)
            elif key in self.valid_fields:
                if 'keywords' == key:
                    keywords = json_dict[key].encode('utf-8').split(',')
                    keywords = map(string.strip, keywords)
                    fea = self.update_features(fea, key, keywords, False)
                else:
                    fea = self.update_features(fea, key, [json_dict[key]], False)
        return fea

    def extract_label(self, json_dict, fea):
        active_time = self.default_value_map['mean_active_time']
        if 'activeTime' in json_dict:
            active_time = int(json_dict['activeTime'])
        label = int((active_time - self.min_active_time) / self.section)
        fea.add_label(label)
        return fea

    def convert_examples(self):
        example_id = 1
        for line in sys.stdin:
            json_dict = json.loads(line)
            fea = self.extract_feature(json_dict)
            fea = self.extract_labe(json_dict, fea)
            fea.set_example_id(example_id)
            fea.to_example()
            example_id += 1

    def split_list(self):
        sessions_dict = dict()
        appendable_dict = dict()
        lines = sys.stdin.readlines()
        session = []
        for line in lines:
            line_json = json.loads(line)
            if 'userId' not in line_json:
                logging.warning("missing userId.")
                continue
            if 'time' not in line_json:
                logging.warning("missing time.")
                continue
            if 'sessionStart' not in line_json:
                logging.warning("missing sessionStart.")
                continue
            if 'sessionStop' not in line_json:
                logging.warning("missing sessionStop.")
                continue
            user_id = line_json['userId']
            cur_time = line_json['time']
            session_start = line_json['sessionStart']
            session_stop = line_json['sessionStop']
            if user_id not in appendable_dict:
                appendable_dict[user_id] = False
            if not session_start:
                if not appendable_dict[user_id]:
                    continue
                else:
                    last_time = sessions_dict[user_id][-1][-1]['time']
                    if cur_time < last_time:
                        logging.warning("%s: cur time %d less than last time."
                                %(user_id, cur_time))
                        continue
                    sessions_dict[user_id][-1].append(line_json)
            if session_start:
                if appendable_dict[user_id]:
                    sessions_dict[user_id].pop()
                else:
                    appendable_dict[user_id] = True
                if user_id not in sessions_dict:
                    sessions_dict[user_id] = []
                sessions_dict[user_id].append([line_json])
            if session_stop:
                appendable_dict[user_id] = False
        for user in sessions_dict.keys():
            if appendable_dict[user]:
                sessions_dict[user].pop()
            if len(sessions_dict[user]) == 0:
                del sessions_dict[user]
        return sessions_dict

    def convert_list_examples(self):
        sessions_dict = self.split_list()
        list_example_id = 1
        for user in sessions_dict:
            for session in sessions_dict[user]:
                item_feature_list = []
                for json_dict in session:
                    fea = self.extract_feature(json_dict)
                    fea = self.extract_label(json_dict, fea)
                    fea.set_example_id(list_example_id)
                    item_feature_list.append(fea)
                list_example_id += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--task',
            help = 'convert_examples/convert_list_examples')
    parser.add_argument('--features_map_file', help = 'features_map_file')
    parser.add_argument('--valid_fields_file', help = 'valid_fields_file')
    parser.add_argument('--variable_len_fields_file',
            help = 'variable_len_fields_file')
    parser.add_argument('--defalt_value_file', help = 'default_value_file')
    args = parser.parse_args()
    data_parser = DataParser()
    elif args.task == 'convert_examples':
        data_parser.convert_examples(args.features_map)
    elif args.task == 'convert_list_examples':
        data_parser.convert_list_examples(args.features_map)
