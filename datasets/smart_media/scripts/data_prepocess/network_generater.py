import argparse
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class NetworkGenerater:
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

    def print_schema_header(self, fout):
        fout.write('name: \"data_feed\"\n')
        fout.write('info {\n')
        fout.write('\tname: \"id\"\n')
        fout.write('\tvalue_type: \"uint\"\n')
        fout.write('}\n')

    def print_schema_slot(self, fout, name, value_type, allow_empty, max_len):
        fout.write('slot {\n')
        fout.write('\tname: ' + name + '\n')
        fout.write('\tvalue_type: ' + value_type + '\n')
        fout.write('\tallow_empty: ' + allow_empty + '\n')
        fout.write('\tmax_len: ' + max_len + '\n')
        fout.write('}\n')
    
    def generate_schema(self, features_map_file, schema_file):
        f = open(features_map_file)
        schema_f = open(schema_file, 'w')
        tokens = f.readline().strip().split('\t')
        field_num = int(tokens[0])
        self.print_schema_header(schema_f)
        for field_ind in xrange(field_num):
            tokens = f.readline().strip().split('\t')
            field_name = tokens[0]
            if field_name not in self.multiple_fields:
                self.print_schema_slot(schema_f, '\"' + field_name + '\"',
                        '\"uint\"', 'true', 1)
            else:
                self.print_schema_slot(schema_f, '\"' + field_name + '\"',
                        '\"uint\"', 'true', 10)
            feature_num = int(tokens[1])
            for feature_ind in xrange(feature_num):
                line = f.readline()
        self.print_schema_slot(schema_f, '\"label\"', '\"float\"', 'true', 1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--task', help = 'schema',
            required = True)
    parser.add_argument('--features_map')
    args = parser.parse_args()
    network_generater = NetworkGenerater()
    if args.task == 'schema':
        network_generater.generate_schema()
