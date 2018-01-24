import argparse
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class NetworkGenerater:
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

    def get_max_len(self, field_name, is_rnn):
        if field_name.replace('_bound', '') in self.multiple_fields:
            if is_rnn:
                return 50 * 1000
            return 50
        else:
            if is_rnn:
                return 1 * 1000
            return 1

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
    
    def generate_schema(self, features_map_file, schema_file, is_rnn):
        self.__init__()
        f = open(features_map_file)
        schema_f = open(schema_file, 'w')
        tokens = f.readline().strip().split('\t')
        field_num = int(tokens[0])
        self.print_schema_header(schema_f)
        for field_ind in xrange(field_num):
            tokens = f.readline().strip().split('\t')
            field_name = tokens[0]
            self.print_schema_slot(schema_f, '\"' + field_name + '\"',
                    '\"uint\"', 'true', str(self.get_max_len(field_name,
                        is_rnn)))
            if is_rnn and field_name in self.multiple_fields:
                self.print_schema_slot(schema_f, '\"' + field_name + '_bound\"',
                        '\"uint\"', 'true', str(self.get_max_len(field_name,
                            is_rnn)))
            feature_num = int(tokens[1])
            for feature_ind in xrange(feature_num):
                line = f.readline()
        self.print_schema_slot(schema_f, '\"label\"', '\"float\"', 'true',
                str(self.get_max_len('label', is_rnn)))

    def tidy(self, raw_str):
        str_list = raw_str.strip().split(', ')
        tidy_str = '\n\t\t'
        for ind in xrange(len(str_list)):
            if ind > 0 and (ind + 1) % 6 != 1:
                tidy_str += ', '
            tidy_str += str_list[ind]
            if ind > 0 and (ind + 1) % 6 == 0 and ind < len(str_list) - 1:
                tidy_str += ',\n\t\t'
        if len(str_list) % 6 != 0 and ind < len(str_list) - 1:
            tidy_str += ', '
        return tidy_str

    def print_train_slots(self, fout, field_name_list, is_rnn):
        field_num = len(field_name_list)
        fout.write('slots = {\n')
        fout.write('\t\"valtype\": [%s, \"float\"],\n' %(
            self.tidy(', '.join(['\"uint\"'] * field_num))))
        fout.write('\t\"name\": [%s, \"label\"],\n' %(
            self.tidy(', '.join(['\"%s\"' %(
                field_name) for field_name in field_name_list]))))
        fout.write('\t\"allow_empty\": [%s, 0],\n' %(
            self.tidy(', '.join(['1'] * field_num))))
        fout.write('\t\"max_seq_len\": [%s, %s],\n' %(
            self.tidy(', '.join([str(self.get_max_len(field_name, is_rnn)) for \
                    field_name in field_name_list])), str(self.get_max_len(
                        'label', is_rnn))))
        fout.write('\t\"fixed_size\": [%s, 1]}\n\n' %(
            self.tidy(', '.join(['0'] * field_num))))

    def print_data_feed(self, fout, field_name_list, data_feed_name, is_shuffle):
        fout.write('%s = Feed({\n' % data_feed_name)
        fout.write( '\t\"type\": \"AdvancedClassify\",\n')
        fout.write('\t\"name\": \"%s\",\n' % data_feed_name)
        fout.write('\t\"slots\": slots,\n')
        fout.write('\t\"infoslots\": infoslots,\n')
        if is_shuffle:
            fout.write('\t\"sampler\": train_sampler,\n')
            fout.write('\t\"param\": {\"shuffle_data\": True},\n')
        else:
            fout.write('\t\"sampler\": test_sampler,\n')
            fout.write('\t\"param\": {\"shuffle_data\": False},\n')

        fout.write('\t\"top_dict\": {\n')
        fout.write('\t\"query\": [%s, \"label\"]}})\n\n' %(self.tidy(', '.join(
            ['\"in_%s\"' %(field_name) for field_name in field_name_list]))))

    def generate_data_feed(self, features_map_file, data_feed_file, is_rnn):
        self.__init__()
        fea_f = open(features_map_file)
        tokens = fea_f.readline().strip().split('\t')
        field_num = int(tokens[0])
        field_name_list = []
        for field_ind in xrange(field_num):
            tokens = fea_f.readline().strip().split('\t')
            field_name = tokens[0]
            field_name_list.append(field_name)
            if is_rnn and field_name in self.multiple_fields:
                field_name_list.append(field_name + '_bound')
            feature_num = int(tokens[1])
            for feature_ind in xrange(feature_num):
                line = fea_f.readline()
        data_f = open(data_feed_file, 'w')
        self.print_train_slots(data_f, field_name_list, is_rnn)
        self.print_data_feed(data_f, field_name_list, 'train_data_feed', True)
        self.print_data_feed(data_f, field_name_list, 'test_data_feed', False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--task', help = 'schema/data_feed',
            required = True)
    parser.add_argument('--features_map')
    parser.add_argument('--schema_file')
    parser.add_argument('--data_feed_file')
    parser.add_argument('--is_rnn', action = 'store_true', default = False)
    args = parser.parse_args()
    network_generater = NetworkGenerater()
    if args.task == 'schema':
        network_generater.generate_schema(args.features_map, args.schema_file,
                args.is_rnn)
    elif args.task == 'data_feed':
        network_generater.generate_data_feed(args.features_map,
                args.data_feed_file, args.is_rnn)
