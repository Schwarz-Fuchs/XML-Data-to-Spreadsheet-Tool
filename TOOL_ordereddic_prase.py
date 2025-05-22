import collections as colle


class ordereddic_prase:

    def __init__(self):
        pass
    
    def begin_with_at(self,str):
        '''
        判断字符串的第一个字符是否是‘@’,用于判断解析值是否是xml标签
        :param str: 输入需要判断的字符串
        :return:  bool
        '''
        list_str = list(str)
        if list_str[0] == "@":
            return True
        else:
            return False

    def data_flatten_lit(self,val):
        """
        （类json）orderdic字典 全展开函数,此方法不考虑下级数据类型，适合不需要展开到底，直接以字符串拼接此节点下内容直接输出的任务
        :param value:   用于迭代的参数，value可以是字典，可以是字典list，可以是值
        :return: 迭代出的下一级 value
        """
        if isinstance(val, dict):
            for ck, cv in val.items():
                if self.begin_with_at(ck):
                    continue
                else:
                    yield from self.data_flatten_lit(cv)
        elif isinstance(val, (list, tuple, set)):
            for item in val:
                yield from self.data_flatten_lit(item)
        elif isinstance(val, (str, int, float, bool, complex, bytes)) or val is None:
            yield val
    

    def dict_flatten_full(self,key_path,value, ignore_list=[], stop_list=[], con_s='/',
                              basic_types=(str, int, float, bool, bytes)):   # value可能是字典，可能是list，可能是值
        '''
        （类json）orderdic字典 全展开 函数，输入orderdic数据类型,返回格式为字典对应的[(路径，值)，（路径，值）...],此方法考虑多种下级数据类型，适合需要将路径展开到底获取每一个独立值的任务
        :param key_path: 展开路径的起点，如果从原始dict开始则应设定值为''
        :param value:   用于迭代的参数，value可以是字典，可以是字典list，可以是值
        :param ignore_list: 不进入（字段）输出路径的节点修饰的 list   (例 <head 修饰1=’...‘ 修饰2=’...‘> 这些修饰全都加上路径就长到没法读了)
        :param stop_list:   不解析节点的 list
        :param con_s: （字段）路径分隔符
        :return: [(路径，值)，（路径，值）...]
        '''
        if isinstance(value, dict):  # 如果当前value类型是字典
            for next_key, next_value in value.items():
                if self.begin_with_at(next_key) and next_key not in ignore_list:
                    key_path = key_path + '[' + next_key + '=' + next_value + ']'
            for next_key, next_value in value.items():
                if next_key not in stop_list:
                    yield from self.dict_flatten_full(con_s.join([key_path, next_key]).lstrip('_'), next_value,
                                                     ignore_list, stop_list)  # key_path更新到下一级组合
                else:
                    yield (str(key_path) + '/' + next_key, '子表')

        elif isinstance(value, (list, tuple, set)):  # 如果当前value类型是list
            have_dic = False
            have_value = False
            for item in value:  # 对于list中的每一个下级value（item)
                if isinstance(item, (dict, colle.OrderedDict)):  # 如果list里是字典，则向下一级
                    have_dic = True
                elif isinstance(item, basic_types):
                    have_value = True  # !!!有可能list里面不是dic而是值，此时我们认定 出现了同一标题下的重复,对值做合并处理！！！！！！

            if have_dic == True and have_value == False:
                for item in value:
                    yield from self.dict_flatten_full(key_path, item, ignore_list, stop_list)
            elif have_dic == False and have_value == True and value is not None:
                yield (str(key_path), str(';'.join('%s' %i for i in value)))  ##使用的分隔符是半角,将list中内容转为字符后拼接;
            elif have_dic == True and have_value == True:  ##如果list中既有值又有字典，则对其进行分离
                '''
                #不分离字典和值的方法，会将本级所有东西全部当做字符串处理 
                new_list=[]
                for item in value:
                    new_list.append(str(item))
                yield (str(key_path), str(';'.join(new_list)))
                '''
                list_basic_value = []
                list_dict = []
                for item in value:
                    if isinstance(item, (dict, colle.OrderedDict)):
                        list_dict.append(item)
                    if isinstance(item, basic_types):
                        list_basic_value.append(item)
                yield (str(key_path), str(';'.join(list_basic_value)))
                for item in list_dict:
                    yield from self.dict_flatten_full(key_path, item, ignore_list, stop_list)

        elif isinstance(value, basic_types) or value is None:
            yield (str(key_path), str(value))

    
    def safe_query_lit(self,dic, sub_branch_path):
        '''
        不考虑存在list情况下ordereddic 按照 sub_branch_path 索引得到的子分支值的方法, 适合不需要展开到底，直接以字符串拼接此节点下内容直接输出的任务
        :param dic:  输入的ordereddic
        :param sub_branch_path: 子分支的路径
        :return: 按照 sub_branch_path索引得到的子分支
        '''
        path_list = sub_branch_path.split('/')
        try:
            current = dic
            for key in path_list:
                current = current[key]
        except:
            current = ''
        return current

    def safe_deep_full(self,value, sub_branch_path, ignore_list=[],
                        basic_types=(str, int, float, bool, complex, bytes)):  # 安全查询，并保留上一级的信息
        '''
        查找按照输入的ordereddic 按照 sub_branch_path 索引得到的子分支 ordereddic
        本方法用于生成子表，系适合orderdic的安全索引方法，返回[value,key_path]，其中value是按路径查询到的dict或者dictlist，key_path保存了查询结果上一级的路径，用于子表到主表的映射
        :param value:  输入的ordereddic
        :param sub_branch_path: 子分支的路径
        :param ignore_list: 不需要进入输出路径的节点修饰   ['@??','@??'...]
        :param basic_types:  不需要解析的节点  ['table','???'...]
        :return: [value, key_path] value为按照 sub_branch_path索引得到的子分支 ,key_path保存了查询结果上一级的路径
        '''
        key_list = sub_branch_path.split('/')
        key_path = key_list[-2]
        for key in key_list:
            if key == key_list[-1]:
                if isinstance(value, dict):
                    for next_key, next_value in value.items():
                        if self.begin_with_at(next_key) and next_key not in ignore_list:
                            key_path = key_path + '[' + next_key + '=' + next_value + ']'
                        else:
                            key_path = key_path
            else:
                key_path = key_path
            try:
                if isinstance(value[key], basic_types) or value[key] is None:  # 如果value下一级查询得到的是值，则只需要value更新为该值
                    value = value[key]
                elif isinstance(value[key], colle.OrderedDict):
                    value = value[key]  # 如果value下一级查询得到的是ordereddict，则value更新为该ordereddict
                elif isinstance(value[key], (list, tuple, set)):  # 如果value下一级查下一级是o(list, tuple, set)
                    new_value_list = []
                    for sub_dic in value[key]:
                        new_value_list.append(sub_dic)
                        value = new_value_list  # 则需将value更新为该new_value_list
                else:
                    value = None
            except:
                value = None
        key_path = key_path + '/' + key_list[-1]
        return [value, key_path]


    ############################################以下为使用本工具时主要调用的方法#####################################################
    
    def extract_main_branch(self,xml_dict, file_path, ignore_list=[], stop_list=[]):
        '''
        本函数用于对xml转化后的order_dict格式数据再进行格式转换,输出结果为[(路径，值)，（路径，值）...]
        :param xml_dict:   xml转出的order_dict格式数据
        :param file_path:  xml文件的路径(可用于表链接)
        :param ignore_list: 不需要进入输出路径的节点修饰   ['@??','@??'...]
        :param stop_list:  不需要解析的节点  ['table','???'...]
        :return: list [(路径，值)，（路径，值）...]
        '''
        data_extracted = list(self.dict_flatten_full('', xml_dict, ignore_list, stop_list))
        data_extracted.append(("file_path", file_path))
        data_extracted.append(("lenth", len(data_extracted)))
        return data_extracted

    def extract_sub_branch(self,xml_dict, branch_path, file_path, ignore_list=[], stop_list=[]):
        '''
        本函数用于对xml转化后的order_dict格式数据 按照branch_path索引到子节点后对子节点部分进行格式转换 ,输出结果为[(路径，值)，（路径，值）...]
        :param dic:   xml转出的order_dict格式数据
        :param branch_path:  子节点路径
        :param file_path:  xml文件的路径(可用于表链接)
        :param ignore_list:  不需要进入输出路径的节点修饰   ['@??','@??'...]
        :param stop_list:  不需要解析的节点  ['table','???'...]
        :return: list [(路径，值)，（路径，值）...]
        '''
        fore_path = self.safe_deep_full(xml_dict, branch_path)[1]
        sub_dic_list = self.safe_deep_full(xml_dict, branch_path)[0]
        if isinstance(sub_dic_list, list):
            sumlist = []
            for dic in sub_dic_list:
                data_extracted = list(self.dict_flatten_full(fore_path, dic, ignore_list, stop_list))
                sumlist = sumlist +data_extracted
            sumlist.append(("file_path", file_path))
            return sumlist

        elif isinstance(sub_dic_list, dict):
            sumlist = list(self.dict_flatten_full(fore_path, sub_dic_list, ignore_list, stop_list))
            sumlist.append(("file_path", file_path))
            return sumlist

        else:
            return [("file_path", file_path)]

    def extract_whole_unit_str(self,xml_dict, branch_path, ignore_list, sep=' '):
        '''
        本函数仅适用于需要把某节点下所有内容拼接为字符串输出的任务，本函数仅输出按路径查询的字符串，如需加入上面data_extract结果，请手动append
        :param xml_dict:  xml转出的order_dict格式数据
        :param branch_path:  子节点路径
        :param sep: 对于子节点下多值情况的分隔符
        :return: 按分隔符拼接的字符串查询结果
        '''
        sub_dict = self.safe_deep_full(xml_dict, branch_path, ignore_list)

        return sub_dict








if __name__=='__main__':
    '''在这里运行数据测试'''
    '''
    file_path=r'F:\医学会XML\FtpDownload\0253-2352\71909.xml'

    replace_list=[['\n','']]
    reg_list=[]
    xml_dict = xml_to_ordereddic().xml_to_dic_standardize(file_path,replace_list,reg_list)

    ignore_list=['@xmlns:mml','@xmlns:xlink','@xmlns:xsi','@dtd-version','@xsi:noNamespaceSchemaLocation'
                 ,'@article-type','@xml:lang']
    stop_list=[]

    extract_list = ordereddic_prase().extract_main_branch(xml_dict, file_path, ignore_list, stop_list)
    for item in extract_list:
        print (item)
    '''












