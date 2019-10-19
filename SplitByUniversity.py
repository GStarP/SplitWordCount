import pymongo
import json
import jieba


# 常量定义
school_file_path = './schools.json'

ban_file_path = './ban.txt'

host = 'mongodb://94.191.110.118'
port = 27017
db_name = 'university_weibo'
col_base = '_weibo_info'

# 读取高校列表字典
school_dict = {}
school_file = open(school_file_path, encoding='UTF-8')
school_file_json = json.load(school_file)
for item in school_file_json['university_list']:
    school_dict[item['id']] = item['name']

# 读取停用词列表
ban_list = [line.strip() for line in open('ban.txt', encoding='UTF-8').readlines()]
ban_list.append(' ')


# 停用词规则
def is_valid(s):
    if s in ban_list:
        return False
    if s.isdigit():
        return False
    if len(s) < 2:
        return False
    return True


# 总词表
total_count_dict = {}

# 连接 MongoDB
client = pymongo.MongoClient(host, port)
print('----连接 ' + host + ':' + str(port) + ' 成功----')
db = client[db_name]
for id in school_dict.keys():
    # 输出文件路径
    out_file_path = './out/' + id + '.txt'
    word_count_dict = {}
    col_name = id + col_base
    col = db[col_name]
    print('----正在处理 ' + col_name + ' 集合----')
    for wb in col.find():
        year = wb['time'][:4]
        print('----正在处理 ' + wb['time'][:10] + ' 的微博----')
        # if wb['time'][:7] != '2019-10':
        if year != '2019' and year != '2018':
            break
        else:
            content = wb['content']
            split_list = jieba.cut(content)
            for word in split_list:
                if is_valid(word):
                    if word not in word_count_dict.keys():
                        word_count_dict[word] = 1
                    else:
                        val = word_count_dict[word]
                        word_count_dict[word] = val + 1

                    if word not in total_count_dict.keys():
                        total_count_dict[word] = 1
                    else:
                        val = total_count_dict[word]
                        total_count_dict[word] = val + 1
    # 将结果输出到文件
    sorted_key = sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True)
    with open(out_file_path, 'w', encoding='UTF-8') as out_file:
        for t in sorted_key:
            key = t[0]
            out_file.write(key + ' ' + str(word_count_dict[key]) + '\n')
    print('---- ' + id + '.txt 输出完毕----')

# 输出总文件
total_sorted_key = sorted(total_count_dict.items(), key=lambda x: x[1], reverse=True)
with open('./out/all.txt', 'w', encoding='UTF-8') as total_out_file:
    for t in total_sorted_key:
        key = t[0]
        total_out_file.write(key + ' ' + str(total_count_dict[key]) + '\n')
print('---- all.txt 输出完毕----')
