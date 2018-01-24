import argparse
import sys
import json
import string
reload(sys)
sys.setdefaultencoding('utf-8')

class FeatureChecker:
    def __init__(self): 
        self.line_key_set = set()
        self.variable_len_fields = set()

    def print_active_time(self):
        print 'active_time\x01%d\x01%f\x01%d\x01%d' %(self.active_time_count,
                self.mean_active_time, self.min_active_time,
                self.max_active_time)

    def print_features_dict(self):
        serial_features_dict = dict()
        for field in self.features:
            serial_features_dict[field] = list(self.features[field])
        print 'features_dict\x01%s' % (json.dumps(serial_features_dict))

    def update_features(self, field, feature_list):
        if field in self.line_key_set:
            self.variable_len_fields.add(field)
        else:
            self.line_key_set.add(field)
        if field not in self.features:
            self.features[field] = set()
        for feature in feature_list:
            self.features[field].add(feature)

    def parse_json(self):
        for line in sys.stdin:
            self.line_key_set.clear()
            line_json = json.loads(line)
            for key in line_json.keys():
                if isinstance(line_json[key], list):
                    if 'author' == key:
                        self.update_features(key, line_json[key])
                    elif 'profile' == key:
                        item_list = line_json[key]
                        for item_json in item_list:
                            item = item_json['item']
                            category_list = item_json['groups']
                            for category_json in category_list:
                                count = category_json['count']
                                category = category_json['group']
                                weight = category_json['weight']
                                self.update_features(category.replace('-', '_'),
                                        [item])
                elif 'activeTime' == key:
                    activeTime = int(line_json[key])
                    self.min_active_time = min(self.min_active_time, activeTime)
                    self.max_active_time = max(self.max_active_time, activeTime)
                    self.mean_active_time = self.mean_active_time \
                            * self.active_time_count + activeTime
                    self.active_time_count += 1
                    self.mean_active_time /= self.active_time_count
                else:
                    key = key.replace('-', '_')
                    if 'keywords' == key:
                        keywords = [kw.strip() for kw in line_json[key].split(',')]
                        self.update_features(key, keywords)
                    else:
                        self.update_features(key, [line_json[key]])
        self.print_active_time()
        self.print_features_dict()
        print 'variable_len_fields\x01%s' %(json.dumps(list(
            self.variable_len_fields)))

    def merge_feature_stat(self):
        self.max_active_time = -sys.maxint - 1
        self.min_active_time = sys.maxint
        self.mean_actime_time = 0.0
        self.active_time_count = 0
        for line in sys.stdin:
            tokens = line.strip().split('\x01')
            flag = tokens[0]
            if flag == 'active_time':
                active_time_count, mean_active_time, min_active_time, \
                        max_active_time = tokens[1:]
                self.mean_active_time = self.mean_active_time \
                        * self.active_time_count + float(mean_active_time) \
                        * int(active_time_count)
                self.active_time_count += int(active_time_count)
                self.mean_active_time /= self.active_time_count
                self.min_active_time = min(self.min_active_time,
                        int(min_active_time))
                self.max_active_time = max(self.max_active_time,
                        int(max_active_time))
            elif flag == 'features_dict':
                json_dict = json.loads(tokens[1])
                for field in json_dict:
                    for feature in json_dict[field]:
                        self.update_features(field, feature)
            elif flag == 'variable_len_fields':
                variable_len_field_list = json.loads(tokens[1])
                self.variable_len_fields.update(variable_len_field_list)
        feature_count = 0
        for field in self.features:
            feature_count += len(self.features[field])
        print str(len(self.features)) + '\x01' + str(feature_count)
        for field in self.features:
            print field + '\x01' + str(len(self.features[field]))
            feature_id = 1
            for feature in self.features[field]:
                if isinstance(feature, unicode):
                    print feature.encode('utf-8') + '\x01' + str(feature_id)
                else:
                    print str(feature) + '\x01' + str(feature_id)
                feature_id += 1
        print len(self.variable_len_fields)
        for variable_len_field in self.variable_len_fields:
            print variable_len_field
        self.print_active_time()

    def split_feature_stat(self, feature_stat_file, features_map_file,
            default_value_file, valid_fields_file, variable_len_fields_file):
        feature_stat_f = open(feature_stat_file, 'w')
        features_map_f = open(features_map_file, 'w')
        default_value_f = open(default_value_file, 'w')
        valid_fields_f = open(valid_fields_file, 'w')
        variable_len_fields_f = open(variable_len_fields_file, 'w')
        # features map
        valid_fields = []
        line = feature_stat_f.readline().strip()
        field_count, feature_count = map(int, line.split('\x01'))
        features_map_f.write(line)
        for field_id in xrange(field_count):
            line = feature_stat_f.readline().strip()
            features_map_f.write(line)
            tokens = line.split('\x01')
            field_name = tokens[0]
            valid_fields.append(field_name)
            cur_feature_count = int(tokens[1])
            for i in xrange(cur_feature_count):
                line = feature_stat_f.readline().strip()
                features_map_f.write(line)
        # valid fields
        valid_fields_f.write(str(len(valid_fields)))
        for valid_field in valid_fields:
            valid_fields_f.write(valid_field)
        # variable length fields
        line = feature_stat_f.readline().strip(f)
        variable_len_fields_f.write(line)
        for i in xrange(int(line)):
            variable_len_field = feature_stat_f.readline().strip()
            variable_len_fields_f.write(variable_len_field)
        # default label value
        flag, active_time_count, mean_active_time, min_active_time, \
                max_active_time = feature_stat_f.readline().strip().split('\x01')
        self.active_time_count = int(active_time_count)
        self.mean_active_time = float(mean_active_time)
        self.min_active_time = int(min_active_time)
        self.max_active_time = int(max_active_time)
        self.section = (self.max_active_time - self.min_active_time) / 20.0
        default_value_f.write('active_time_count\x01%d' %(self.active_time_count))
        default_value_f.write('mean_active_time\x01%f' %(self.mean_active_time))
        default_value_f.write('min_active_time\x01%d' %(self.min_active_time))
        default_value_f.write('max_active_time\x01%d' %(self.max_active_time))
        default_value_f.write('active_time_section\x01%f' %(self.section))
