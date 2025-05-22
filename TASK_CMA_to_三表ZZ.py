from TOOL_xml_read import *
from TOOL_xml_to_ordereddic import *
from TOOL_ordereddic_prase import *
from TOOL_result_generate import *
from tqdm import tqdm
from fnmatch import fnmatch
import  hashlib
import base64
import re
import pandas.errors

import warnings
warnings.simplefilter(action='ignore')



import copy
class bached_xml_prase_ZZ:

    def __init__(self):
        pass

    def base64_encode(self,file_path):
        # 取医学会文件后两位位址
        file_list = file_path.split('\\')
        file_id = str(file_list[-2]) + str(file_list[-1])
        s_en = file_id.encode(encoding='utf-8')
        encodestr = base64.b64encode(s_en)
        qcode_str = 'qk_' + str(encodestr).replace("'", '')
        return qcode_str

    def get_countri_type_from_path(self,zz_line_seq,path_dic):
        if zz_line_seq!=-1:
            path=path_dic[zz_line_seq]

            contrib_type_pattern=re.compile(r"(?<=\[@contrib-type=)[a-zA-Z]+(?=\])")
            corresp_mark_pattern=re.compile(r"(?<=\[@corresp=)[a-zA-Z]+(?=\])")
            content_type_pattern=re.compile(r"(?<=\[@content-type=)[a-zA-Z]+(?=\])")

            contrib_type_list = contrib_type_pattern.findall(path)
            corresp_mark_list = corresp_mark_pattern.findall(path)
            content_type_list = content_type_pattern.findall(path)

            author_role=''
            if len(contrib_type_list)>0:
                contrib_type=str(contrib_type_list[0]).lower()
                if contrib_type == 'author':
                    author_role ='作者'
                elif contrib_type == 'editor':
                    author_role ='编辑'
                elif contrib_type == 'proofreader':
                    author_role ='责任校对'
                elif contrib_type == 'typesetter':
                    author_role ='责任排版'
                elif contrib_type == 'reviewer':
                    author_role ='审阅'
                elif contrib_type == 'speaker':
                    author_role ='报告人'
                elif contrib_type == 'translator':
                    author_role ='译者'
                else:
                    author_role=contrib_type

            if len(corresp_mark_list) > 0:
                corresp_mark = str(corresp_mark_list[0]).lower()
                if corresp_mark == 'yes':
                    author_role = '通讯作者'

            if len(content_type_list) > 0:
                content_type = str(content_type_list[0]).lower()
                if content_type == 'correspauthor':
                    author_role = '通讯作者'

            if '/collab' in path:
                author_role='团体作者'

            return author_role



    def generate_ZZ(self, batch_xml_list):  # ZZ获取对应关系较为复杂，需要多种情况分离
        ZZ_list = []
        feature_list_JAST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'JAST_三表_ZZ')
        feature_list_ABST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'ABST_三表_ZZ')

        for xml_path in tqdm(batch_xml_list):
            ZZ_single_unit = []

            # 对源xml进行字符串替换处理，replace_list为直接匹配并替换，reg_list为正则匹配并替换
            replace_list = [['\n', ''],['\r', ''],['      ',''],#['<sub>',''],['</sub>',''],['<sup>',''],['</sup>',''],
                            ['<italic>',''],['</italic>','']]
            reg_list = [[' +', ' ']]
            xml_dict = xml_to_ordereddic().xml_to_dic_standardize(xml_path, replace_list, reg_list)

            # 解析中文作者信息分支
            ignore_list = ['@xmlns:mml', '@xmlns:xlink', '@xmlns:xsi', '@dtd-version', '@xsi:noNamespaceSchemaLocation'
                , '@article-type']
            stop_list = ['graphic', 'back', 'permissions', 'journal-meta', 'pub-date', 'volume', 'issue', 'fpage',
                         'lpage'
                , 'abstract', 'kwd-group', 'funding-group', 'counts']
            zz_list = ordereddic_prase().extract_main_branch(xml_dict, xml_path, ignore_list, stop_list)

            #print(zz_list)

            # 保留值于路径的对应字典
            path_dic={}
            for i in range(len(zz_list)):
                line_seq=i
                key=str(line_seq)
                path_dic.update({key:str(zz_list[i][0])})
            #print(path_dic)

            # 区分JAST 和 abstract 的两节点
            xsi = prased_path_to_value().get_value_lit(zz_list, '/article/@xsi:noNamespaceSchemaLocation', '')
            have_front = prased_path_to_value().have_node(zz_list, 'front')


            if 'CMA-article' in xsi or have_front is True:  # 若xsi中有CMA-article字样或有front节点，说明文件为CMA JAST 型题录信息 或新版简要题录型数据

                extract_feature = []
                markline = '☆'
                for feature_and_path in feature_list_JAST:
                    feature_name = feature_and_path[0]
                    feature_path = feature_and_path[1]
                    feature_path_fliter = feature_and_path[2]
                    unit_seq = prased_path_to_value().get_value_lit(zz_list,
                                                                    '/article/front/article-meta/contrib-group/aff/#text',
                                                                    feature_path_fliter)

                    if unit_seq != '' and unit_seq is not None:  # 如果有seq标记列,不识别行号，分类号为marked_seq
                        feature_value = prased_path_to_value().get_value_lit(zz_list, feature_path, feature_path_fliter,
                                                                             markline=markline)
                        extract_feature.append((feature_name, feature_value))
                        extract_feature.append(('seq_patten', 'marked_seq'))
                    else:  # 否则记录行号，按照行进行对应
                        feature_value = prased_path_to_value().get_value_lit(zz_list, feature_path, feature_path_fliter,
                                                                             markline=markline)
                        extract_feature.append((feature_name, feature_value))
                        extract_feature.append(('seq_patten', 'line_seq'))

                all_feature_dic = dict(extract_feature)
                for key, value in all_feature_dic.items():
                    if value=='None':
                        all_feature_dic.update({key:''})


                file_path = all_feature_dic['file_path']
                all_feature_dic.update({'QCODE': self.base64_encode(str(all_feature_dic['file_path']))})

                ##在此处对dic内字段进行合并，规范等操作
                splited_ZZ_list = []  # 作者多值拆分
                if all_feature_dic['seq_patten'] == 'marked_seq':  ##有次序标记的类型
                    ##中文作者操作
                    code = prased_path_to_value().sep_string_with_seq(all_feature_dic['CODE'], ';', markline)[0][1]
                    zz_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ'], ';',markline)

                    zz_sur_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_sur'],
                                                                                          ';', markline)
                    zz_given_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_given'],
                                                                                            ';', markline)
                    zz_group_list_marked=prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_group'],
                                                                                            ';', markline)

                    ##添加 sur 和 given 类型结果
                    if len(zz_sur_value_list_marked) == len(zz_given_value_list_marked):
                        for i in range(len(zz_sur_value_list_marked)):
                            lin_seq = zz_sur_value_list_marked[i][0]
                            zz_name = str(zz_sur_value_list_marked[i][1]) + ' ' + str(zz_given_value_list_marked[i][1])
                            if lin_seq!=-1:
                                zz_value_list_marked.append((lin_seq, zz_name))

                    ##添加团体作者类型结果
                    for i in range(len(zz_group_list_marked)):
                        lin_seq=zz_group_list_marked[i][0]
                        zz_name=zz_group_list_marked[i][1]
                        if lin_seq != -1:
                            zz_value_list_marked.append((lin_seq, zz_name))

                    ##按照行值重新对作者进行排序
                    zz_value_list_marked.sort(key=lambda x: int(x[0]))

                    unit_seq_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['zz_unit_seq'], ';',
                                                                                 markline)
                    DWys_LIST_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['DWys_pattern2'], ';',
                                                                                  markline)
                    unit_code_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['unit_code'], ';',
                                                                                  markline)
                    zz_bio = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZJ'], ';', markline)

                    match_list_zz = prased_path_to_value().value_match_by_seq(
                        [zz_value_list_marked, zz_bio, unit_seq_marked], lead=True ,save_line_seq=True)
                    match_list_dw = prased_path_to_value().value_match_by_seq([unit_code_marked, DWys_LIST_marked],
                                                                              lead=True)
                    error_log = ''
                    for i in range(len(match_list_zz)):
                        zz_line_seq=match_list_zz[i][0]
                        zz_name = match_list_zz[i][1][0]
                        zz_bio = match_list_zz[i][1][1]
                        zz_unit_seq = match_list_zz[i][1][2]
                        zz_cx = i + 1
                        dwys = ''

                        ##匹配原始单位dwys
                        for match_dw in match_list_dw:
                            for code in zz_unit_seq:
                                if code == match_dw[0]:
                                    matched_dw_code = match_dw[0]
                                    try:
                                        dwys = dwys + str(match_dw[1][0]) + ';'
                                    except:
                                        if len(match_dw[1]) > 0:
                                            dwys = dwys + str(match_dw[1][0]) + ';'
                                        else:
                                            dwys = dwys
                                        error_log = ('ERROR: ' + str(file_path) + '中文作者单位信息存在问题')
                        if dwys != '':
                            dwys = dwys[:-1]

                        ##获取作者角色 (使用路径匹配方法获取路径修饰值)
                        authors_role=self.get_countri_type_from_path(zz_line_seq, path_dic)

                        dic_new = copy.deepcopy(all_feature_dic)
                        if zz_name != '' and zz_name is not None:
                            dic_new.update({'CODE': code})
                            dic_new.update({'CH': True})
                            dic_new.update({'ZZ': zz_name})
                            dic_new.update({'JS': authors_role})
                            dic_new.update({'ZJ': ';'.join(zz_bio)})
                            dic_new.update({'DWys': dwys})
                            dic_new.update({'zz_unit_seq': ';'.join(zz_unit_seq)})
                            dic_new.update({'CX': str(int(zz_cx))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['DWys_pattern2'];
                            del dic_new['DWys_pattern1'];
                            del dic_new['ZZE_sur'];
                            del dic_new['ZZE_given']
                            del dic_new['DWys_pattern2_en'];
                            del dic_new['DWys_pattern1_en'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)
                    if error_log != '':
                        print(error_log)

                    # 英文作者操作
                    code = prased_path_to_value().sep_string_with_seq(all_feature_dic['CODE'], ';', markline)[0][1]

                    zz_sur_value_list_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZE_sur'],
                                                                                             ';', markline)
                    zz_given_value_list_marked_en = prased_path_to_value().sep_string_with_seq(
                        all_feature_dic['ZZE_given'], ';', markline)
                    zz_group_list_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_group_en'],
                                                                                      ';', markline)
                    #团体作者塞到 sur 里面处理
                    ##添加团体作者类型结果
                    for i in range(len(zz_group_list_marked_en)):
                        lin_seq = zz_group_list_marked_en[i][0]
                        zz_name = zz_group_list_marked_en[i][1]
                        if lin_seq != -1:
                            zz_sur_value_list_marked_en.append((lin_seq, zz_name))

                    zz_sur_value_list_marked_en.sort(key=lambda x: int(x[0]))

                    unit_seq_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['zz_unit_seq_en'],
                                                                                    ';', markline)
                    DWys_LIST_marked_en = prased_path_to_value().sep_string_with_seq(
                        all_feature_dic['DWys_pattern2_en'], ';', markline)
                    unit_code_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['unit_code_en'],
                                                                                     ';', markline)
                    match_list_zz_en = prased_path_to_value().value_match_by_seq(
                        [zz_sur_value_list_marked_en, zz_given_value_list_marked_en, unit_seq_marked_en], lead=True,save_line_seq=True)

                    match_list_dw_en = prased_path_to_value().value_match_by_seq(
                        [unit_code_marked_en, DWys_LIST_marked_en], lead=True)
                    error_log = ''

                    for i in range(len(match_list_zz_en)):
                        zz_line_seq= match_list_zz_en[i][0]
                        zz_name_sur = match_list_zz_en[i][1][0]
                        zz_name_given = match_list_zz_en[i][1][1]
                        zz_unit_seq_en = ';'.join(match_list_zz_en[i][1][2])
                        zz_name=(str(zz_name_sur) + ' ' + str(''.join(zz_name_given))).lstrip().rstrip()
                        zz_cx_en = i + 1
                        dwys = ''
                        ##匹配dwys
                        for match_dw in match_list_dw_en:
                            for code in zz_unit_seq_en:
                                if code == match_dw[0]:
                                    try:
                                        dwys = dwys + str(match_dw[1][0]) + ';'
                                    except:
                                        if len(match_dw[1]) > 0:
                                            dwys = dwys + str(match_dw[1][0]) + ';'
                                        else:
                                            dwys = dwys
                                        error_log = ('ERROR: ' + str(file_path) + '英文作者单位信息存在问题')

                        if dwys != '':
                            dwys = dwys[:-1]
                        dic_new = copy.deepcopy(all_feature_dic)
                        ##匹配作者角色
                        authors_role=self.get_countri_type_from_path(zz_line_seq, path_dic)

                        if zz_name_sur != '' and zz_name_sur is not None:
                            dic_new.update({'CODE': code})
                            dic_new.update({'CH': False})
                            dic_new.update({'ZZ': zz_name})
                            dic_new.update({'JS': authors_role})
                            dic_new.update({'ZJ': ''})
                            dic_new.update({'DWys': dwys})
                            dic_new.update({'zz_unit_seq':zz_unit_seq_en})
                            dic_new.update({'CX': str(int(zz_cx_en))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['DWys_pattern2'];
                            del dic_new['DWys_pattern1'];
                            del dic_new['ZZE_sur'];
                            del dic_new['ZZE_given']
                            del dic_new['DWys_pattern2_en'];
                            del dic_new['DWys_pattern1_en'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)
                    if error_log != '':
                        print(error_log)


                else:  # 没有次序标记的数据
                    ##中文作者处理
                    code = prased_path_to_value().sep_string_with_seq(all_feature_dic['CODE'], ';', markline)[0][1]
                    zz_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ'], ';',
                                                                                      markline)
                    zz_sur_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_sur'],
                                                                                          ';', markline)
                    zz_given_value_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_given'],
                                                                                            ';', markline)
                    zz_group_list_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_group'],
                                                                                      ';', markline)
                    ##sur + given处理
                    if len(zz_sur_value_list_marked) == len(zz_given_value_list_marked):
                        for i in range(len(zz_sur_value_list_marked)):
                            lin_seq = zz_sur_value_list_marked[i][0]
                            zz_name = str(zz_sur_value_list_marked[i][1]) + ' ' + str(zz_given_value_list_marked[i][1])
                            if lin_seq!=-1:
                                zz_value_list_marked.append((lin_seq, zz_name))
                                #print(zz_value_list_marked)
                    ##添加团体作者类型结果
                    for i in range(len(zz_group_list_marked)):
                        lin_seq = zz_group_list_marked[i][0]
                        zz_name = zz_group_list_marked[i][1]
                        if lin_seq != -1:
                            zz_value_list_marked.append((lin_seq, zz_name))

                    ##按照行值重新对作者进行排序
                    zz_value_list_marked.sort(key=lambda x: int(x[0]))

                    DWys_LIST_marked = prased_path_to_value().sep_string_with_seq(all_feature_dic['DWys_pattern1'], ';',
                                                                                  markline)
                    zz_bio = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZJ'], ';', markline)
                    match_list = prased_path_to_value().value_match_by_seq(
                        [zz_value_list_marked, zz_bio, DWys_LIST_marked], lead=True,save_line_seq=True)

                    for i in range(len(match_list)):
                        zz_line_seq=match_list[i][0]
                        zz_name=match_list[i][1][0]
                        zj=';'.join(match_list[i][1][1])
                        dwys=';'.join(match_list[i][1][2])

                        ##匹配作者角色

                        authors_role=self.get_countri_type_from_path(zz_line_seq, path_dic)

                        dic_new = copy.deepcopy(all_feature_dic)
                        if zz_name != '' and zz_name is not None:
                            dic_new.update({'CODE': code})
                            dic_new.update({'CH': True})
                            dic_new.update({'ZZ': zz_name})
                            dic_new.update({'JS': authors_role})
                            dic_new.update({'ZJ': zj})
                            dic_new.update({'DWys': dwys})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['DWys_pattern2'];
                            del dic_new['DWys_pattern1'];
                            del dic_new['ZZE_sur'];
                            del dic_new['ZZE_given']
                            del dic_new['DWys_pattern2_en'];
                            del dic_new['DWys_pattern1_en'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束

                            if dic_new['ZZ']!='' and dic_new['ZZ'] is not None:
                                ZZ_single_unit.append(dic_new)

                    ZZ_single_unit.append('$SEP$')

                    ##英文作者处理
                    code = prased_path_to_value().sep_string_with_seq(all_feature_dic['CODE'], ';', markline)[0][1]
                    zz_sur_list_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZE_sur'], ';',
                                                                                       markline)
                    zz_giver_list_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZE_given'],
                                                                                         ';', markline)
                    zz_group_list_marked_en = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZ_group_en'],
                                                                                         ';', markline)
                    # 团体作者塞到 sur 里面处理
                    ##添加团体作者类型结果
                    for i in range(len(zz_group_list_marked_en)):
                        lin_seq = zz_group_list_marked_en[i][0]
                        zz_name = zz_group_list_marked_en[i][1]
                        if lin_seq != -1:
                            zz_sur_list_marked_en.append((lin_seq, zz_name))

                    zz_sur_list_marked_en.sort(key=lambda x: int(x[0]))

                    DWys_LIST_marked_en = prased_path_to_value().sep_string_with_seq(
                        all_feature_dic['DWys_pattern1_en'], ';', markline)
                    match_lis_en = prased_path_to_value().value_match_by_seq(
                        [zz_sur_list_marked_en, zz_giver_list_marked_en, DWys_LIST_marked_en], lead=True, save_line_seq=True)
                    for i in range(len(match_lis_en)):
                        zz_line_seq=match_lis_en[i][0]
                        zz_name_sur=str(match_lis_en[i][1][0])
                        zz_name=(str(match_lis_en[i][1][0]) + ' ' + str(''.join(match_lis_en[i][1][1]))).lstrip().rstrip()
                        dwys= ';'.join(match_lis_en[i][1][2])
                        ##匹配作者角色
                        #print(zz_name)
                        authors_role=self.get_countri_type_from_path(zz_line_seq, path_dic)

                        dic_new = copy.deepcopy(all_feature_dic)
                        if zz_name_sur != '' and zz_name_sur is not None:
                            dic_new.update({'CODE': code})
                            dic_new.update({'CH': False})
                            dic_new.update({'ZZ': zz_name})
                            dic_new.update({'JS': authors_role})
                            dic_new.update({'ZJ': ''})
                            dic_new.update({'DWys':dwys})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['DWys_pattern2'];
                            del dic_new['DWys_pattern1'];
                            del dic_new['ZZE_sur'];
                            del dic_new['ZZE_given']
                            del dic_new['DWys_pattern2_en'];
                            del dic_new['DWys_pattern1_en'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_single_unit.append(dic_new)

                    ##作者单位修正：补充缺失的英文单位名称(注意这里使用的是ZZ_single_unit)
                    sep_point = 0
                    for i in range(len(ZZ_single_unit)):
                        if ZZ_single_unit[i] == '$SEP$':
                            sep_point = i
                    if len(ZZ_single_unit) > 2 and sep_point < len(ZZ_single_unit) - 2:
                        try:
                            if ZZ_single_unit[0] != '$SEP$':
                                pre_chi_dw = 'init'
                                cur_chi_dw = 'init'
                                en_dw_add = ''
                                same_dw_list = []
                                for en_cursor in range(sep_point + 1, len(ZZ_single_unit)):
                                    chi_cursor = int(en_cursor) - int(sep_point) - 1
                                    cur_chi_dw = ZZ_single_unit[int(chi_cursor)]['DWys']
                                    if pre_chi_dw == 'init':
                                        same_dw_list.append(en_cursor)
                                        pre_chi_dw = cur_chi_dw
                                        continue
                                    elif pre_chi_dw == cur_chi_dw:
                                        same_dw_list.append(en_cursor)
                                        pre_chi_dw = cur_chi_dw
                                        continue
                                    elif pre_chi_dw != cur_chi_dw:
                                        for i in same_dw_list:
                                            if ZZ_single_unit[i]['DWys'] != '' and ZZ_single_unit[i][
                                                'DWys'] is not None:
                                                en_dw_add = ZZ_single_unit[i]['DWys']
                                                break
                                        for i in same_dw_list:
                                            ZZ_single_unit[i]['DWys'] = en_dw_add
                                        same_dw_list = []
                                        en_dw_add = ''
                                        pre_chi_dw = cur_chi_dw
                                        continue
                                for i in same_dw_list:
                                    if ZZ_single_unit[i]['DWys'] != '' and ZZ_single_unit[i]['DWys'] is not None:
                                        en_dw_add = ZZ_single_unit[i]['DWys']
                                        break
                                for i in same_dw_list:
                                    ZZ_single_unit[i]['DWys'] = en_dw_add
                        except:
                            pass


                            '''
                            dw_1 = ZZ_single_unit[0]['DWys']
                            for i in range(sep_point + 1, len(ZZ_single_unit)-1):
                                if ZZ_single_unit[i]['DWys'] != '' and ZZ_single_unit[i]['DWys'] is not None:
                                    eng_dw_lack = 0
                            for i in range(1, sep_point-1):
                                if ZZ_single_unit[i]['DWys'] != dw_1:
                                    same_dw = 0
                            if eng_dw_lack == 1 and same_dw == 1:
                                for i in range(sep_point + 2, len(ZZ_single_unit)-1):
                                    ZZ_single_unit[i].update({'DWys': dw_add})        
                            ##判断最后一位是否是没有单位给出的编辑，是否需要补充英文单位：
                            if ZZ_single_unit[len(ZZ_single_unit)-1]['DWys'] != '' and ZZ_single_unit[len(ZZ_single_unit)-1]['DWys'] is not None:
                                eng_dw_lack = 0
                            if eng_dw_lack == 1 and same_dw == 1:
                                ZZ_single_unit[len(ZZ_single_unit)-1].update({'DWys': dw_add})
                            '''
                    ZZ_single_unit.remove('$SEP$')
                    ##作者单位修正结束

                    for item in ZZ_single_unit:
                        ZZ_list.append(item)


            elif ('CMA-abstract' in xsi and have_front is False) or xsi==''  or xsi is None:  # 若xsi中有CMA-abstract字样，且没有front节点 说明文件为CMA 老版简要题录型信息
                extract_feature = []
                ##规则表待补充
                for feature_and_path in feature_list_ABST:
                    feature_name = feature_and_path[0]
                    feature_path = feature_and_path[1]
                    feature_path_fliter = feature_and_path[2]
                    feature_value = prased_path_to_value().get_value_lit(zz_list, feature_path, feature_path_fliter)
                    # 此形式下多值分隔符需规范
                    if feature_value != '' and feature_value is not None:
                        feature_value = feature_value.replace('|', ';')
                    extract_feature.append((feature_name, feature_value))
                    extract_feature.append(('seq_patten', 'no_seq'))
                all_feature_dic = dict(extract_feature)
                for key, value in all_feature_dic.items():
                    if value=='None':
                        all_feature_dic.update({key:''})

                all_feature_dic.update({'QCODE': self.base64_encode(str(all_feature_dic['file_path']))})
                ##在此处对dic内字段进行合并，规范等操作  abstract格式木大对应，您自求多福吧
                ##中文作者处理
                zz_value_list = all_feature_dic['ZZ'].split(';')
                dw_value_list = all_feature_dic['DWys'].split(';')
                if len(zz_value_list)>0 and zz_value_list[0]!='None':
                    for i in range(len(zz_value_list)):
                        if len(zz_value_list) == len(dw_value_list):
                            dic_new = copy.deepcopy(all_feature_dic)
                            dic_new.update({'CH': True})
                            dic_new.update({'ZZ': zz_value_list[i]})
                            dic_new.update({'DWys': dw_value_list[i]})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['Dwys_en'];
                            del dic_new['ZZE'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)
                        else:
                            dic_new = copy.deepcopy(all_feature_dic)
                            dic_new.update({'CH': True})
                            dic_new.update({'ZZ': zz_value_list[i]})
                            dic_new.update({'DWys': ';'.join(dw_value_list)})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['Dwys_en'];
                            del dic_new['ZZE'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)

                ##英文作者处理
                zz_value_list_en = all_feature_dic['ZZE'].split(';')
                dw_value_list_en = all_feature_dic['Dwys_en'].split(';')
                if len(zz_value_list_en)>0 and zz_value_list_en[0]!='None':
                    for i in range(len(zz_value_list_en)):
                        if len(zz_value_list_en) == len(dw_value_list_en):
                            dic_new = copy.deepcopy(all_feature_dic)
                            dic_new.update({'CH': False})
                            dic_new.update({'ZZ': zz_value_list_en[i]})
                            dic_new.update({'DWys': dw_value_list_en[i]})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            del dic_new['Dwys_en'];
                            del dic_new['ZZE'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            ##dic内字段规范结束
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)
                        else:
                            dic_new = copy.deepcopy(all_feature_dic)
                            dic_new.update({'CH': False})
                            dic_new.update({'ZZ': zz_value_list_en[i]})
                            dic_new.update({'DWys': ';'.join(dw_value_list_en)})
                            dic_new.update({'CX': str(int(i + 1))})
                            dic_new.update({'file_path': str(xml_path)})
                            ##dic内字段规范结束
                            del dic_new['Dwys_en'];
                            del dic_new['ZZE'];
                            del dic_new['unit_code'];
                            del dic_new['unit_code_en'];
                            del dic_new['zz_unit_seq_en']
                            if dic_new['ZZ'] != '' and dic_new['ZZ'] is not None:
                                ZZ_list.append(dic_new)
            else:
                print('ERROR:xml文件 ' + str(xml_path) + 'ZZ表未能进行解析分类')

        df_ZZ = pd.DataFrame(ZZ_list)
        df_ZZ=df_ZZ.replace('None', '')
        df_ZZ=df_ZZ.fillna('')

        df_ZZ=df_ZZ.dropna(axis=0, subset=["ZZ"])

        return df_ZZ

    def generate_ZZ_comtable(self, df_ZZ):

        qcode_list_res=[]
        zzname_list=[]
        zzname_en_list=[]
        zz_dw_list=[]
        zz_dw_en_list=[]

        qcode_list=df_ZZ['QCODE'].values.tolist()
        distinct_qcode_list=list(set(qcode_list))

        for qcode in distinct_qcode_list:
            try:
                sub_zz_df = df_ZZ[df_ZZ['QCODE'] == qcode]
                sub_zz_df['CX'] = pd.to_numeric(sub_zz_df['CX'])
                sub_zz_df_ordered_ch = sub_zz_df[sub_zz_df['CH'] == 1].sort_values(by=['CX'], ascending=[True])
                sub_zz_df_ordered_en = sub_zz_df[sub_zz_df['CH'] == 0].sort_values(by=['CX'], ascending=[True])

                distinct_zz_ch_list = []
                zz_ch_list = sub_zz_df_ordered_ch['ZZ'].values.tolist()
                for item in zz_ch_list:
                    if item not in distinct_zz_ch_list and item.lstrip().rstrip() != '':
                        distinct_zz_ch_list.append(item)
                zz_comb = ';'.join(distinct_zz_ch_list)

                distinct_dw_ch_list = []
                dw_ch_list = sub_zz_df_ordered_ch['DWys'].values.tolist()
                for item in dw_ch_list:
                    if item not in distinct_dw_ch_list and item.lstrip().rstrip() != '':
                        distinct_dw_ch_list.append(item)
                dw_comb = ';'.join(distinct_dw_ch_list)

                distinct_zz_en_list = []
                zz_en_list = sub_zz_df_ordered_en['ZZ'].values.tolist()
                for item in zz_en_list:
                    if item not in distinct_zz_en_list and item.lstrip().rstrip() != '':
                        distinct_zz_en_list.append(item)
                zz_en_comb = ';'.join(distinct_zz_en_list)

                distinct_dw_en_list = []
                dw_en_list = sub_zz_df_ordered_en['DWys'].values.tolist()
                for item in dw_en_list:
                    if item not in distinct_dw_en_list and item.lstrip().rstrip() != '':
                        distinct_dw_en_list.append(item)
                dw_en_comb = ';'.join(distinct_dw_en_list)

                qcode_list_res.append(qcode)
                zzname_list.append(zz_comb)
                zzname_en_list.append(zz_en_comb)
                zz_dw_list.append(dw_comb)
                zz_dw_en_list.append(dw_en_comb)

            except Exception as e:
                print("作者表多只合并错误:",e)
                print("错误qcode:",qcode)

        comb_dic={'QCODE':qcode_list_res,'ZZ':zzname_list,'ZZE':zzname_en_list,'DWYS':zz_dw_list,'DWYSE':zz_dw_en_list}
        df_comb=pd.DataFrame(data=comb_dic)
        return df_comb




if __name__=='__main__':
    ##在这里输入解析参数
    xml_path = r'J:\医学会xml_new\debug'  ##在这里输入需要解析的包含xml的文件夹

    time_range = ["", ""]  ##在这里输入需要解析的文件修改时间区间[上限，下限] 时间格式为"%Y/%M/%D %h:%m:%s 若上限或下限不设置，请置''
    # xml_path=r'G:\医学会xml\FtpDownload\debug2'
    batch_size = 50000

    sever = '10.192.64.163'  ##服务器地址
    user_name = 'sa'  ##MSSQL数据库用户名
    password = 'Devdb.@163'  ##MSSQL用户密码
    database = '医学会转三表_ZY'  ##数据转出的目标数据库名称（需要提前创建）

    table_name_ZZ = '三表_ZZ_CMA'  ##数据转出的目标表 作者表

    fail_data_continue = False  ##是否从batch断点继续运行（报错修正后使用，继续运行未完成的batch）
    '''
    #要输出scv时
    result_path='结果表'  #生成csv的地址
    '''
    all_file_path = []
    print('正在获取目录下所有的xml路径......')
    xml_all = xml_read().findxml_lit_time_fliter(xml_path, all_file_path, time_range)
    batch_file_path = batch_prepare().split_batch(all_file_path, batch_size)
    print('正在初始化表格......', end=' ')

    for i in range(len(batch_file_path)):
        print('当前正在处理第'+str(i+1)+'批，所有数据共'+str(len(batch_file_path))+'批。')
        print('*****************************')
        df_ZZ = bached_xml_prase_ZZ().generate_ZZ(batch_file_path[i])
        df_ZZ_checked = batch_prepare().column_check('规则表/feature_check_CMA.xlsx', '三表_ZZ', df_ZZ)

        bached_xml_prase_ZZ().generate_ZZ_comtable(df_ZZ_checked)

        #result_to_sqlsever().data_insert(df_ZZ_checked, user_name, password, sever, database, table_name_ZZ)
        print('作者表完成写入')