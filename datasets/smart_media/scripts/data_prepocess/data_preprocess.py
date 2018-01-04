import argparse
import json
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

    def add_feature(self, field_id, feature_id):
        self.feature_slots[field_id].append(feature_id)

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
            if i > 0:
                example_str += ' '
            example_str += str(self.labels[i])
        print example_str

class DataParser:
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
                'concept', 'keywords', 'addressa-tag', 'sentiment', 'pageclass', \
                'adressa-access', 'category', 'company', 'adressa-importance',
                'language', 'person']
        self.features = dict()
        self.max_active_time = -sys.maxint - 1
        self.min_active_time = sys.maxint
        self.mean_active_time = 0.0
        self.active_time_count = 0
        self.section = 0

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
                    if 'profile' == key:
                        item_list = line_json[key]
                        for item_json in item_list:
                            item = item_json['item']
                            category_list = item_json['groups']
                            for category_json in category_list:
                                count = category_json['count']
                                category = category_json['group']
                                weight = category_json['weight']
                                if 'keywords' == category:
                                    keywords = item.split(',')
                                    for kw in keywords:
                                        self.update_features(category, kw)
                                else:
                                    self.update_features(category, item)
                elif 'activeTime' == key:
                    activeTime = int(line_json[key])
                    self.min_active_time = min(self.min_active_time, activeTime)
                    self.max_active_time = max(self.max_active_time, activeTime)
                    self.mean_active_time = self.mean_active_time \
                            * self.active_time_count + activeTime
                    self.active_time_count += 1
                    self.mean_active_time /= self.active_time_count
                elif key in self.valid_fields:
                    self.update_features(key, line_json[key])
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
                tokens = f.readline().strip().split('\t')
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

    def convert_examples(self, features_map_file):
        self.load_features_map(features_map_file)
        example_id = 1
        for line in sys.stdin:
            has_label = False
            fea = Feature(example_id, len(self.field_ind_map))
            line_json = json.loads(line)
            for key in line_json.keys():
                if isinstance(line_json[key], list):
                    if 'profile' == key:
                        item_list = line_json[key]
                        for item_json in item_list:
                            feature = item_json['item']
                            field_list = item_json['groups']
                            for field_json in field_list:
                                occurrence = field_json['count']
                                field = field_json['group']
                                weight = field_json['weight']
                        field_ind = self.field_ind_map[field]
                        if 'keyword' == field:
                            keywords = feature.encode('utf-8').split(',')
                            for kw in keywords:
                                feature_id = self.feature_id_maps[field_ind][kw]
                                fea.add_feature(field_ind, faeture_id)
                        else:
                            feature_id = self.feature_id_maps[ \
                                    field_ind][feature.encode('utf-8')]
                            fea.add_feature(field_ind, feature_id)
                elif key in self.valid_fields:
                    field_ind = self.field_ind_map[key]
                    feature_id = \
                            self.feature_id_maps[field_ind][str(line_json[key])]
                    fea.add_feature(field_ind, feature_id)
                else :
                    if 'activeTime' == key:
                        activeTime = int(line_json[key])
                        fea.add_label(int((
                            activeTime - self.min_active_time) / self.section))
                        has_label = True
            if not has_label:
                fea.add_label(int((self.mean_active_time
                    - self.min_active_time) / self.section))
            fea.to_example()
            example_id += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--task', help = 'parse_json/merge_features/convert_examples',
            required = True)
    parser.add_argument('--features_map')
    args = parser.parse_args()
    data_parser = DataParser()
    if args.task == 'parse_json':
        data_parser.parse_json()
    elif args.task == 'merge_features':
        data_parser.merge_features_dict()
    elif args.task == 'convert_examples':
        data_parser.convert_examples(args.features_map)
