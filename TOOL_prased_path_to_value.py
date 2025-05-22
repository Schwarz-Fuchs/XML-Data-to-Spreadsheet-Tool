from fnmatch import fnmatch
import pandas as pd

class prased_path_to_value:



    def __init__(self):
        pass

    def read_feature(self,feature_excel_path, sheet_name):
        '''
        从excel中读取映射表
        :param feature_excel_path: 映射表路径
        :param sheet_name: 映射表sheet名
        :return: list[[最终映射名list],[xml路径list]]
        '''
        data = pd.read_excel(feature_excel_path, sheet_name=sheet_name, engine='openpyxl', keep_default_na=False)
        ####如果需要更多备份路径，在此处添加
        path_list = data.values.tolist()

        return path_list

    def have_node(self,extract_list,node_name):
        got_node=False
        for value_pair in extract_list:
            node_list=value_pair[0].split('/')
            if node_name in node_list:
                got_node=True
                break
            else:
                continue
        return got_node

    def get_value_lit(self,extract_list,path_pattern,path_flitter,sep=';',strict=False,markline='',save_origin_path=False):
        '''
        依照给出路径从展开的orderedic结果列表中 获取对应值（路径存在多值情况使用分隔符隔开）
        :param extract_list:  使用ordereddic_prase()中方法得到的 路径-值 展开表
        :param path_pattern:  查询路径 在非严格条件下（strict=False） 使用中间必要节点查找所有匹配的值 ;严格条件下（strict=True）需要提供完整路径    两种方式下路径修饰都是匹配可选项，提供则进一步过滤
        :param path_flitter:  需要路径中排除的字符 list
        :param sep:           多值情况下使用的分隔符
        :param strict:        是否进行严格路径查找
        :param markline:      行号标记符 默认不记录行号，但如果这一参数非空，则查找值会在开头标记行号 格式如： <markline>3<markline><对应值>
        :return: 路径查找到的值，多值情况按照分隔符进行分隔
        '''
        #没有给出path的直接返回空值
        if path_pattern=='' or path_pattern==None:
            if save_origin_path==False:
                return ''
            else:
                return {'':''}

        input_path_list = path_pattern.split('/')
        temp = []
        for item in input_path_list:
            item = item.replace('[', '*')
            item = item.replace(']', '')
            temp.append(item)
        if strict==False:
            pattern = '*'.join(temp)  #此情况下允许路径中间出现节点和节点修饰缺失（使用尾+中间必要节点识别）
            #pattern = pattern+'*'
        else:
            pattern = '?'.join(temp)  #此情况不允许节点缺失（允许节点修饰缺失）

        if path_flitter=='' or path_flitter==None:
            path_flitter=[]
        else:
            path_flitter=path_flitter.split(';')

        value_string=''
        ori_path=''
        for i in range(len(extract_list)):
            if fnmatch(extract_list[i][0],pattern):
                match=1
                for flitter in path_flitter:
                    if str(flitter) in str(extract_list[i][0]):
                        match=0
                if match==1:
                    if value_string=='':
                        if markline=='':
                            value_string=str(extract_list[i][1])
                            ori_path=str(extract_list[i][0])
                        else:
                            value_string=str(markline)+str(i)+str(markline)+str(extract_list[i][1])
                            ori_path = str(extract_list[i][0])
                    else:
                        if markline=='':
                            value_string=str(value_string)+str(sep)+str(extract_list[i][1])
                            ori_path = str(ori_path)+str(sep)+str(extract_list[i][0])
                        else:
                            value_string=str(value_string)+str(sep)+str(markline)+str(i)+str(markline)+str(extract_list[i][1])
                            ori_path = str(ori_path) + str(sep) + str(extract_list[i][0])

        if save_origin_path==False:
            return value_string
        else:
            return {value_string:ori_path}

    def sep_string_with_seq(self,value_string_marked,sep,markline):
        '''
        将记录了行号的多值字段拆分 得到[(行号，值)...]类型list
        :param value_string_marked: 有行号标记的多值字段
        :param sep:   多值分隔符
        :param markline: 行号标记符
        :return: 拆分后的表格 格式为：[(行号，值)，(行号，值)...]
        '''
        seq_sep=[]
        string_list=value_string_marked.split(sep)
        try:
           seq_line=-1
           for string in string_list:
               string_sep_list=string.split(markline)
               if len(string_sep_list)>1:
                   seq_line=string_sep_list[1]
                   value=string_sep_list[2]
               else:
                   seq_line=seq_line
                   value=string_sep_list[0]
               seq_sep.append((seq_line,value))
           return seq_sep
        except:
            print(string_list)


    def value_match_by_seq(self,seq_sep_list_ordered,lead=True,save_line_seq=False):
        '''
        通过行号进行字段归类 字段2,3,4会归入字段1以下
        :param seq_sep_list_ordered: seq_sep_list_ordered输入需求： [字段1：[(行号，值)，(行号，值)...],字段2：[(行号，值)，(行号，值)...],字段3[(行号，值)，(行号，值)...]，...] 其中行号需要从小到大排列
        :param lead:  当设置为True时，以字段1为基准，后续字段均只取用行号在字段1两值之间值进行对应；字段1非空，未能对应的值直接舍弃
                      当设置为False时，依照行号，按字段次序填入，每行至少一个字段非空
        :return: 对应后的list  [[字段1值，[字段2值,...]，[字段3值,...]],[字段1值，[字段2值...]，[字段3值...]].....]
        '''

        if lead==True:
            match_list=[]
            for i in range(len(seq_sep_list_ordered[0])):
                single_unit=[]
                if i<len(seq_sep_list_ordered[0])-1:
                    lead_value=seq_sep_list_ordered[0][i][1]
                    lead_seq_minium=seq_sep_list_ordered[0][i][0]
                    lead_seq_maxium=seq_sep_list_ordered[0][i+1][0]
                else:
                    lead_value=seq_sep_list_ordered[0][i][1]
                    lead_seq_minium=seq_sep_list_ordered[0][i][0]
                    lead_seq_maxium=900000000000

                single_unit.append(lead_value)

                for j in range(1,len(seq_sep_list_ordered)):
                    follow_value=[]
                    for k in range(len(seq_sep_list_ordered[j])):
                        seq_line=seq_sep_list_ordered[j][k][0]
                        if int(lead_seq_minium)<int(seq_line)<int(lead_seq_maxium):   #如果行值在lead字段的行数行号中间
                            follow_value.append(seq_sep_list_ordered[j][k][1])  #更新follow value
                        elif int(seq_line)<int(lead_seq_minium):
                            continue
                        else:    #行值大于lead上限会停止查找，所以输入表的行号必须都是从小到大排序的
                            break
                    single_unit.append(follow_value)
                if save_line_seq==False:
                    match_list.append(single_unit)
                else:
                    match_list.append((lead_seq_minium,single_unit))
        else:
            #后面需要的时候再补充
            match_list=[]

        return match_list




if __name__=='__main__':

    seged_list=prased_path_to_value().sep_string_with_seq(r'%50%昆明医科大学第一附属医院放射科，昆明\u3000650000;\u3000昆明医科大学第三附属医院放射科，昆明\u3000650000',';','%')
    print(seged_list)

    seq_list=[[(1,'a'),(5,'b'),(9,'c')],[(3,'FOR a'),(6,'for b'),(12,'for c')],[(4,'aaa'),(12,'ccc'),(12,'ddd'),(16,'fff')]]
    result=prased_path_to_value().value_match_by_seq(seq_list)
    print(result)

