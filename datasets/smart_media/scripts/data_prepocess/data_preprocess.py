import argparse
import json
import logging
import string
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Feature:
    def __init__(self, example_id, field_num):
        self.id = example_id
        self.feature_slots = []
        self.labels = []
        for i in xrange(field_num):
            self.feature_slots.append([])

    def add_feature(self, field_ind, feature_id):
        self.feature_slots[field_ind].append(feature_id)

    def add_label(self, label):
        self.labels.append(label)

    def to_example(self):
        example_str = str(self.id) + ';'
        for i in xrange(len(self.feature_slots)):
            for j in xrange(len(self.feature_slots[i])):
                if j > 0:
                    example_str += ' '
                example_str += str(self.feature_slots[i][j])
            example_str += ';'
        for i in xrange(len(self.labels)):
            for j in xrange(len(self.labels[i])):
                if j > 0:
                    example_str += ' '
                example_str += str(self.labels[i][j])
        print example_str

class ListFeature(Feature):
    def __init__(self, list_example_id, field_num):
        Feature.__init__(self, list_example_id, field_num)
        self.feature_bounds = dict()

    def add_feature_bound(self, field_ind):
        if field_ind not in self.feature_bounds:
            self.feature_bounds[field_ind] = []
        self.feature_bounds[field_ind].append(len(self.feature_slots[field_ind]))

    def to_example(self):
        example_str = str(self.id) + ';'
        for i in xrange(len(self.feature_slots)):
            for j in xrange(len(self.feature_slots[i])):
                if j > 0:
                    example_str += ' '
                example_str += str(self.feature_slots[i][j])
            example_str += ';'
            if i in self.feature_bounds:
                for j in xrange(len(self.feature_bounds[i])):
                    if j > 0:
                        example_str += ' '
                    example_str += str(self.feature_bounds[i][j])
                example_str += ';'
        for i in xrange(len(self.labels)):
            for j in xrange(len(self.labels[i])):
                if j > 0:
                    example_str += ' '
                example_str += str(self.labels[i][j])
            if i < len(self.labels) - 1:
                example_str += ';'
        print example_str

class DataParser:
    def format_fields(self, fields):
        for i in xrange(len(fields)):
            fields[i] = fields[i].replace('-', '_')

    def __init__(self):
        # activeTime, site, referrerUrl, id, eventId, profile, canonicalUrl
        # userId, publishtime, url, title, time
        self.valid_fields = ['category1', 'os', 'classification', 'taxonomy',
            'referrerHostClass', 'entity', 'concept', 'keywords', 'adressa-tag',
            'city', 'sentiment', 'author', 'referrerSearchEngine', 'deviceType',
            'location', 'pageclass', 'sessionStop', 'adressa-access', 'category',
            'company', 'adressa-importance', 'referrerSocialNetwork', 'language',
            'country', 'region', 'person', 'sessionStart']
        self.multiple_fields = ['classification', 'taxonomy', 'entity', \
                'concept', 'keywords', 'adressa-tag', 'sentiment', 'author', \
                'location', 'pageclass', 'adressa-access', 'category', \
                'company', 'adressa-importance', 'language', 'person', 'acronym']
        self.format_fields(self.valid_fields)
        self.format_fields(self.multiple_fields)
        self.features = dict()
        self.max_active_time = -sys.maxint - 1
        self.min_active_time = sys.maxint
        self.mean_active_time = 0.0
        self.active_time_count = 0
        self.section = 0

    def define_feature_type(self, fea):
        self.fea = fea

    def update_features(self, field, feature):
        if field not in self.features:
            self.features[field] = set()
        self.features[field].add(feature)

    def print_active_time(self):
        print 'time\x01%d\x01%f\x01%d\x01%d' %(self.active_time_count,
                self.mean_active_time, self.min_active_time,
                self.max_active_time)

    def print_features_dict(self):
        serial_features_dict = dict()
        for field in self.features:
            serial_features_dict[field] = list(self.features[field])
        print 'feature\x01%s' % (json.dumps(serial_features_dict))

    def parse_json(self):
        for line in sys.stdin:
            line_json = json.loads(line)
            for key in line_json.keys():
                if isinstance(line_json[key], list):
                    if 'author' == key:
                        for author in line_json[key]:
                            self.update_features(key, author)
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
                                        item)
                elif 'activeTime' == key:
                    activeTime = int(line_json[key])
                    self.min_active_time = min(self.min_active_time, activeTime)
                    self.max_active_time = max(self.max_active_time, activeTime)
                    self.mean_active_time = self.mean_active_time \
                            * self.active_time_count + activeTime
                    self.active_time_count += 1
                    self.mean_active_time /= self.active_time_count
                elif key.replace('-', '_') in self.valid_fields:
                    if 'keywords' == key:
                        keywords = line_json[key].split(',')
                        for kw in keywords:
                            self.update_features(key.replace('-', '_'),
                                    kw.strip())
                    else:
                        self.update_features(key.replace('-', '_'),
                                line_json[key])
        self.print_active_time()
        self.print_features_dict()

    def merge_features_dict(self):
        self.max_active_time = -sys.maxint - 1
        self.min_active_time = sys.maxint
        self.mean_actime_time = 0.0
        self.active_time_count = 0
        for line in sys.stdin:
            tokens = line.strip().split('\x01')
            flag = tokens[0]
            if flag == 'time':
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
            elif flag == 'feature':
                json_dict = json.loads(tokens[1])
                for field in json_dict:
                    for feature in json_dict[field]:
                        self.update_features(field, feature)
        feature_count = 0
        for field in self.features:
            feature_count += len(self.features[field])
        print str(len(self.features)) + '\t' + str(feature_count)
        for field in self.features:
            print field + '\t' + str(len(self.features[field]))
            feature_id = 1
            for feature in self.features[field]:
                if isinstance(feature, unicode):
                    print feature.encode('utf-8') + '\t' + str(feature_id)
                else:
                    print str(feature) + '\t' + str(feature_id)
                feature_id += 1
        self.print_active_time()

    def load_features_map(self, features_map_file):
        self.field_ind_map = dict()
        self.feature_id_maps = []
        f = open(features_map_file)
        field_count, feature_count = map(int, f.readline().strip().split('\t'))
        for field_id in xrange(field_count):
            tokens = f.readline().strip().split('\t')
            field_name = tokens[0]
            cur_feature_count = int(tokens[1])
            self.field_ind_map[field_name] = field_id
            self.feature_id_maps.append(dict())
            for i in xrange(cur_feature_count):
                tokens = f.readline().rstrip().split('\t')
                feature_name = tokens[0]
                feature_id = int(tokens[1])
                self.feature_id_maps[field_id][feature_name] = feature_id
        flag, active_time_count, mean_active_time, min_active_time, \
                max_active_time = f.readline().strip().split('\x01')
        self.active_time_count = int(active_time_count)
        self.mean_active_time = float(mean_active_time)
        self.min_active_time = int(min_active_time)
        self.max_active_time = int(max_active_time)
        self.section = (self.max_active_time - self.min_active_time) / 20.0

    def add_field_feature(self, field, feature, is_need_bound, is_need_uniq):
        field_ind = self.field_ind_map[field]
        for feature_name in feature:
            if not isinstance(feature_name, unicode):
                feature_name = str(feature_name)
            feature_id = self.feature_id_maps[field_ind][ \
                    feature_name.encode('utf-8')]
            if is_need_uniq and \
                    feature_id in self.fea.feature_slots[field_ind]:
                continue
            self.fea.add_feature(field_ind, feature_id)
        if is_need_bound:
            self.fea.add_feature_bound(field_ind)

    def extract_features(self, json_dict, is_need_bound):
        for key in json_dict.keys():
            key = key.replace('-', '_')
            if isinstance(json_dict[key], list):
                if 'author' == key:
                    self.add_field_feature(key, json_dict[key], is_need_bound,
                            True)
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
                                self.add_field_feature(field, [feature],
                                        is_need_bound, True)
                            else:
                                self.add_field_feature(field, [feature],
                                        is_need_bound, False)
            elif key in self.valid_fields:
                if 'keywords' == key:
                    keywords = json_dict[key].encode('utf-8').split(',')
                    keywords = map(string.strip, keywords)
                    self.add_field_feature(key, keywords, is_need_bound, False)
                else:
                    self.add_field_feature(key, [json_dict[key]], is_need_bound,
                            False)

    def extract_labels(self, json_list):
        labels = []
        for json_dict in json_list:
            active_time = self.mean_active_time
            if 'activeTime' in json_dict:
                active_time = int(json_dict['activeTime'])
            labels.append(
                    int((active_time - self.min_active_time) / self.section))
        self.fea.add_label(labels)

    def convert_examples(self, features_map_file):
        self.load_features_map(features_map_file)
        example_id = 1
        for line in sys.stdin:
            fea = Feature(example_id, len(self.field_ind_map))
            self.define_feature_type(fea)
            line_json = json.loads(line)
            self.extract_features(line_json, False)
            self.extract_labels([line_json])
            self.fea.to_example()
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

    def convert_list_examples(self, features_map_file):
        sessions_dict = self.split_list()
        self.load_features_map(features_map_file)
        list_example_id = 1
        for user in sessions_dict:
            for session in sessions_dict[user]:
                list_fea = ListFeature(list_example_id, len(self.field_ind_map))
                self.define_feature_type(list_fea)
                for event_json in session:
                    self.extract_features(event_json, True)
                self.extract_labels(session)
                self.fea.to_example()
                list_example_id += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--task',
            type = int,
            help = '1:parse_json/2:merge_features/3:convert_examples' \
                    + '/4:convert_list_examples',
            required = True)
    parser.add_argument('--features_map')
    args = parser.parse_args()
    data_parser = DataParser()
    task_map = {1 : 'parse_json', 2 : 'merge_features', 3 : 'convert_examples',
            4 : 'convert_list_examples'}
    task = task_map[args.task]
    if task == 'parse_json':
        data_parser.parse_json()
    elif task == 'merge_features':
        data_parser.merge_features_dict()
    elif task == 'convert_examples':
        data_parser.convert_examples(args.features_map)
    elif task == 'convert_list_examples':
        data_parser.convert_list_examples(args.features_map)
