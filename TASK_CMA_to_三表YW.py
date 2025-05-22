from TOOL_xml_read import *
from TOOL_xml_to_ordereddic import *
from TOOL_ordereddic_prase import *
from TOOL_result_generate import *
from tqdm import tqdm
from fnmatch import fnmatch
from TOOL_xml_read import *
import re
import base64

import copy
class bached_xml_prase_YW:

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

    def YW_feature_concat(self, all_feature_dic):
        # 生成标准引文
        standard_YW = ''

        if all_feature_dic['publiaction_type'] == 'journal' and all_feature_dic['publication-format'] == 'print': #常规刊物
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            # 4.引文编号
            pub_type = '[J]'
            if all_feature_dic['source'] != '' and all_feature_dic['source'] is not None:
                standard_YW = standard_YW + pub_type + '.' + all_feature_dic['source']
                # 年卷期
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            if all_feature_dic['J'] != '' and all_feature_dic['J'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['J']
            if all_feature_dic['Q'] != '' and all_feature_dic['Q'] is not None:
                standard_YW = standard_YW + '(' + all_feature_dic['Q']
                if all_feature_dic['issue-part'] != '' and all_feature_dic['issue-part'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['issue-part']
                if all_feature_dic['supplement'] != '' and all_feature_dic['supplement'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['supplement']
                standard_YW = standard_YW+')'

            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'journal' and all_feature_dic['publication-format'] =='online-only': #网络刊物
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW +''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            # 4.引文编号
            pub_type = '[J/OL]'
            if all_feature_dic['source'] != '' and all_feature_dic['source'] is not None:
                standard_YW = standard_YW + pub_type + '.' + all_feature_dic['source']
            # 年卷期
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            if all_feature_dic['J'] != '' and all_feature_dic['J'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['J']
            if all_feature_dic['Q'] != '' and all_feature_dic['Q'] is not None:
                standard_YW = standard_YW + '(' + all_feature_dic['Q']
                if all_feature_dic['issue-part'] != '' and all_feature_dic['issue-part'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['issue-part']
                if all_feature_dic['supplement'] != '' and all_feature_dic['supplement'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['supplement']
                standard_YW = standard_YW+')'
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']

            # uri
            if all_feature_dic['uri'] != '' and all_feature_dic['uri'] is not None:
                standard_YW = standard_YW+'.'+all_feature_dic['uri']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'journal' and all_feature_dic['publication-format'] =='electronic': #电子刊物
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW +''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            # 4.引文编号
            pub_type = '[J/CD]'
            if all_feature_dic['source'] != '' and all_feature_dic['source'] is not None:
                standard_YW = standard_YW + pub_type + '.' + all_feature_dic['source']
            # 年卷期
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            if all_feature_dic['J'] != '' and all_feature_dic['J'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['J']
            if all_feature_dic['Q'] != '' and all_feature_dic['Q'] is not None:
                standard_YW = standard_YW + '(' + all_feature_dic['Q']
                if all_feature_dic['issue-part'] != '' and all_feature_dic['issue-part'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['issue-part']
                if all_feature_dic['supplement'] != '' and all_feature_dic['supplement'] is not None:
                    standard_YW = standard_YW + ' '+all_feature_dic['supplement']
                standard_YW = standard_YW+')'
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']

            # uri
            if all_feature_dic['uri'] != '' and all_feature_dic['uri'] is not None:
                standard_YW = standard_YW+'.'+all_feature_dic['uri']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'book' and all_feature_dic['publication-format'] =='print': #编著
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            #2.5章节
            if all_feature_dic['chapter-title'] != '' and all_feature_dic['chapter-title'] is not None:
                standard_YW = standard_YW  + all_feature_dic['chapter-title']+'.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            pub_type = '[M]'
            if all_feature_dic['source'] != '' and all_feature_dic['source'] is not None:
                standard_YW = standard_YW + pub_type + '.' + all_feature_dic['source']
            #4.版本，出版商，出版年
            if all_feature_dic['publisher-loc'] != '' and all_feature_dic['publisher-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['publisher-loc']
            if all_feature_dic['publisher-name'] != '' and all_feature_dic['publisher-name'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['publisher-name']

            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
            #页码
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'newspaper'  and all_feature_dic['publication-format'] =='print': #报纸
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            # 4.引文编号与来源
            pub_type = '[N]'
            if all_feature_dic['source'] != '' and all_feature_dic['source'] is not None:
                standard_YW = standard_YW + pub_type + '.' + all_feature_dic['source']
            # 5.引用时间
            if all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']

        elif all_feature_dic['publiaction_type'] == 'newspaper' and all_feature_dic['publication-format'] =='online-only': #报纸（在线数据库）
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            if all_feature_dic['etal'] != '' and all_feature_dic['etal'] is not None:
                standard_YW = standard_YW + ' ' + all_feature_dic['etal']
            standard_YW = standard_YW + '.'
            # 3.引文标题
            standard_YW = standard_YW + all_feature_dic['YW_TM']
            # 4.标识
            pub_type = '[EB/OL]'
            standard_YW=standard_YW+pub_type+'.'
            # 5.引用时间
            if all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
            if all_feature_dic['date-in-citation-access']!= '' and all_feature_dic['date-in-citation-access'] is not None:
                standard_YW= standard_YW+'['+all_feature_dic['date-in-citation-access']+']'
            # 6.uri
            if all_feature_dic['uri'] != '' and all_feature_dic['uri'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['uri']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'standard': #标准
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            #3.条例名称
            if all_feature_dic['gov'] != '' and all_feature_dic['gov'] is not None:
                standard_YW = standard_YW+ all_feature_dic['gov']
            elif all_feature_dic['std'] != '' and all_feature_dic['std'] is not None:
                standard_YW = standard_YW + all_feature_dic['std']
            #4.标识
            pub_type = '[S]'
            standard_YW=standard_YW+pub_type+'.'
            #出版
            if all_feature_dic['publisher-loc'] != '' and all_feature_dic['publisher-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['publisher-loc']
            if all_feature_dic['publisher-name'] != '' and all_feature_dic['publisher-name'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['publisher-name']
            #出版时间
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
            standard_YW=standard_YW+'.'

        elif all_feature_dic['publiaction_type'] == 'confproc' and all_feature_dic['publication-format'] =='print': #会议
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            ##会议或会议中出现文献
            pub_type = '[C]'
            if all_feature_dic['YW_TM']=='' or all_feature_dic['YW_TM'] is None:
                if all_feature_dic['conf-name']!='' or all_feature_dic['conf-name'] is not None:
                    standard_YW=standard_YW+all_feature_dic['conf-name']
                standard_YW=standard_YW+pub_type
            else:
                standard_YW=standard_YW+all_feature_dic['YW_TM']
                standard_YW = standard_YW + pub_type + '.'
                if all_feature_dic['conf-name']!='' or all_feature_dic['conf-name'] is not None:
                    standard_YW=standard_YW+all_feature_dic['conf-name']
            #会议时间地点，主办方
            if all_feature_dic['conf-date']!='' or all_feature_dic['conf-date'] is not None:
                standard_YW=standard_YW+'.'+all_feature_dic['conf-date']
            if all_feature_dic['conf-loc']!='' or all_feature_dic['conf-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['conf-loc']
            if all_feature_dic['conf-sponsor']!='' or all_feature_dic['conf-sponsor'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['conf-sponsor']
            # 5.引用时间
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
                if all_feature_dic['date-in-citation-access']!= '' and all_feature_dic['date-in-citation-access'] is not None:
                    standard_YW= standard_YW+'['+all_feature_dic['date-in-citation-access']+']'
           #页码
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'confproc' and all_feature_dic['publication-format'] =='online-only': #会议(在线数据库）
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            ##会议或会议中出现文献
            pub_type = '[C/OL]'
            if all_feature_dic['YW_TM']=='' or all_feature_dic['YW_TM'] is None:
                if all_feature_dic['conf-name']!='' or all_feature_dic['conf-name'] is not None:
                    standard_YW=standard_YW+all_feature_dic['conf-name']
                standard_YW=standard_YW+pub_type
            else:
                standard_YW=standard_YW+all_feature_dic['YW_TM']
                standard_YW = standard_YW + pub_type + '.'
                if all_feature_dic['conf-name']!='' or all_feature_dic['conf-name'] is not None:
                    standard_YW=standard_YW+all_feature_dic['conf-name']
            #会议时间地点，主办方
            if all_feature_dic['conf-date']!='' or all_feature_dic['conf-date'] is not None:
                standard_YW=standard_YW+'.'+all_feature_dic['conf-date']
            if all_feature_dic['conf-loc']!='' or all_feature_dic['conf-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['conf-loc']
            if all_feature_dic['conf-sponsor']!='' or all_feature_dic['conf-sponsor'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['conf-sponsor']
            # 5.引用时间
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
                if all_feature_dic['date-in-citation-access']!= '' and all_feature_dic['date-in-citation-access'] is not None:
                    standard_YW= standard_YW+'['+all_feature_dic['date-in-citation-access']+']'
           #页码
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'
            #uri
            # 6.uri
            if all_feature_dic['uri'] != '' and all_feature_dic['uri'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['uri']
            standard_YW = standard_YW + '.'


        elif all_feature_dic['publiaction_type'] == 'report': #报告
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            #报告名称
            if all_feature_dic['YW_TM'] != '' and all_feature_dic['YW_TM'] is not None:
                standard_YW=standard_YW+'.'+all_feature_dic['YW_TM']
            #文献类型
            pub_type = '[R]'
            standard_YW=standard_YW+'[R]'
            # 4.版本，出版商，出版年
            if all_feature_dic['publisher-loc'] != '' and all_feature_dic['publisher-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['publisher-loc']
            if all_feature_dic['publisher-name'] != '' and all_feature_dic['publisher-name'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['publisher-name']

            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
            # 页码
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'

        elif all_feature_dic['publiaction_type'] == 'thesis': #学位论文
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            # 题目名称
            if all_feature_dic['YW_TM'] != '' and all_feature_dic['YW_TM'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['YW_TM']
            # 文献类型
            pub_type = '[D]'
            standard_YW = standard_YW + '[D]'
            # 4.版本，出版商，出版年
            if all_feature_dic['publisher-loc'] != '' and all_feature_dic['publisher-loc'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['publisher-loc']
            if all_feature_dic['publisher-name'] != '' and all_feature_dic['publisher-name'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['publisher-name']

            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']
            # 页码
            if all_feature_dic['Y'] != '' and all_feature_dic['Y'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['Y']
            standard_YW = standard_YW + '.'


        elif all_feature_dic['publiaction_type'] == 'patent': #专利
            # 1.序号
            standard_YW = standard_YW + all_feature_dic['LABLE']
            # 2.作者
            if all_feature_dic['ZZ'] != '' and all_feature_dic['ZZ'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZ'].replace(';', ',')
            elif all_feature_dic['ZZE'] != '' and all_feature_dic['ZZE'] is not None:
                standard_YW = standard_YW + all_feature_dic['ZZE'].replace(';', ',')
            elif all_feature_dic['collab'] != '' and all_feature_dic['collab'] is not None:
                standard_YW = standard_YW + all_feature_dic['collab'].replace(';', ',')
            else:
                standard_YW = standard_YW + ''
            # 题目名称
            if all_feature_dic['YW_TM'] != '' and all_feature_dic['YW_TM'] is not None:
                standard_YW = standard_YW + '.' + all_feature_dic['YW_TM']
            # 文献类型
            pub_type = '[P]'
            standard_YW = standard_YW + '[P]'
            # 4.专利地区，专利号
            if all_feature_dic['patent_country'] != '' and all_feature_dic['patent_country'] is not None:
                standard_YW = standard_YW + ':' + all_feature_dic['patent_country']
                if all_feature_dic['patent_code'] != '' and all_feature_dic['patent_code'] is not None:
                    standard_YW = standard_YW + ',' + all_feature_dic['patent_code']
            else:
                if all_feature_dic['patent_code'] != '' and all_feature_dic['patent_code'] is not None:
                    standard_YW = standard_YW + ':' + all_feature_dic['patent_code']
            #专利时间
            if all_feature_dic['N'] != '' and all_feature_dic['N'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['N']
            elif all_feature_dic['date-in-citation'] != '' and all_feature_dic['date-in-citation'] is not None:
                standard_YW = standard_YW + ',' + all_feature_dic['date-in-citation']

        else:
            pass
        return standard_YW

    def generate_YW(self, batch_xml_list):
        YW_list = []
        feature_list_JAST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'JAST_三表_YW')
        feature_list_ABST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'ABST_三表_YW')

        for xml_path in tqdm(batch_xml_list):

            replace_list = [['\n', ''],['\r', ''],['      ',''],['<sub>',''],['</sub>',''],['<sup>',''],['</sup>',''],
                            ['<italic>',''],['</italic>','']]
            reg_list = [[' +', ' ']]
            xml_dict = xml_to_ordereddic().xml_to_dic_standardize(xml_path, replace_list, reg_list)

            # 解析全部分支
            ignore_list = ['@xmlns:mml', '@xmlns:xlink', '@xmlns:xsi', '@dtd-version', '@xsi:noNamespaceSchemaLocation'
                , '@article-type']  # 不进入路径的修饰符
            stop_list = ['graphic', 'permissions','inline-graphic','trans-mixed-citation']  # 不进行解析的节点
            main_list = ordereddic_prase().extract_main_branch(xml_dict, xml_path, ignore_list, stop_list)
            # 区分JAST 和 abstract 的两节点
            xsi = prased_path_to_value().get_value_lit(main_list, '/article/@xsi:noNamespaceSchemaLocation', '')
            have_front = prased_path_to_value().have_node(main_list, 'front')

            if 'CMA-article' in xsi or have_front is True:  # 若xsi中有CMA-article字样或有front节点，说明文件为CMA JAST 型题录信息 或新版简要题录型数据

                ##这一步直接生成原始拼合引文
                YW_concate_string_list = self.YW_process_on_string_level(xml_path)


                ##后面用于生成拼接引文
                unit_list = []
                single_YW_unit = []
                markline = '☆'

                ##以@id作为引文段落分隔符
                reference_start_patern = '*article*back*ref-list*ref*@id'
                for i in range(len(main_list)):
                    if fnmatch(main_list[i][0], reference_start_patern) == False:
                        single_YW_unit.append(main_list[i])
                    else:
                        unit_list.append(single_YW_unit)
                        single_YW_unit = []
                        single_YW_unit.append(main_list[i])
                unit_list.append(single_YW_unit)
                if len(unit_list) > 0:
                    code = prased_path_to_value().get_value_lit(unit_list[0],'/article/front/article-meta/article-id[@pub-id-type=cma-id]/#text','')

                if len(unit_list) > 1:
                    if len(unit_list)==len(YW_concate_string_list):
                        for i in range(1, len(unit_list)):
                            unit_list[i].append(('/all_yw_concat',str(YW_concate_string_list[i])))
                    else:
                        print('ERROR:'+xml_path +'引文数量解析错误')
                    for i in range(1, len(unit_list)):
                        extract_feature = []
                        for feature_and_path in feature_list_JAST:
                            feature_name = feature_and_path[0]
                            feature_path = feature_and_path[1]
                            feature_path_fliter = feature_and_path[2]
                            feature_value = prased_path_to_value().get_value_lit(unit_list[i], feature_path,
                                                                                 feature_path_fliter, markline=markline)
                            # 非英文作者字段删除行号
                            if feature_name != 'ZZE_sur' and feature_name != 'ZZE_given':
                                pattern = str(markline) + '\d*' + str(markline)
                                feature_value = re.sub(pattern, str(''), str(feature_value))
                            extract_feature.append((feature_name, feature_value))
                        all_feature_dic = dict(extract_feature)
                        for key, value in all_feature_dic.items():
                            if value == 'None':
                                all_feature_dic.update({key: ''})


                        ####在这里进行提取结果处理
                        ##填入qcode

                        all_feature_dic.update({'CODE': code})

                        ##更新lable
                        lable_old = all_feature_dic['LABLE']
                        if '[' not in lable_old and ']' not in lable_old:
                            lable_new = '[' + lable_old.replace('.','') + ']'
                            all_feature_dic.update({'LABLE': lable_new})

                        ##多期更新issue
                        all_feature_dic.update({'Q':  all_feature_dic['Q'].replace(';','/')})

                        ##处理并合并英文名称
                        sur_list = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZE_sur'], sep=';',markline=markline)
                        given_list = prased_path_to_value().sep_string_with_seq(all_feature_dic['ZZE_given'], sep=';',markline=markline)
                        zze_list = prased_path_to_value().value_match_by_seq([sur_list, given_list])
                        zze = ''
                        for item in zze_list:
                            if item[0] != '':
                                zze = zze + item[0]
                            if ' '.join(item[1]) != '':
                                zze = zze + ' ' + ' '.join(item[1])
                            if zze != '':
                                zze = zze + ';'
                        if zze != '':
                            zze = zze[:-1]
                        all_feature_dic.update({'ZZE': zze})
                        del all_feature_dic['ZZE_sur'];
                        del all_feature_dic['ZZE_given']
                        # 生成页码
                        page = ''
                        if all_feature_dic['fpage'] != '' and all_feature_dic['fpage'] is not None:
                            page = page + all_feature_dic['fpage']
                            if all_feature_dic['lpage'] != '' and all_feature_dic['lpage'] is not None:
                                page = page + '-' + all_feature_dic['fpage']
                        all_feature_dic.update({'Y': page})

                        # 生成次序
                        all_feature_dic.update({'CX': i})

                        #拼接标准引文(拼接较为复杂，暂未覆盖所有模式)
                        standard_YW=self.YW_feature_concat(all_feature_dic)


                        all_feature_dic.update({'TM_standard_concat': standard_YW})
                        all_feature_dic.update({'file_path': xml_path})
                        all_feature_dic.update({'QCODE': self.base64_encode(str(all_feature_dic['file_path']))})

                        YW_list.append(all_feature_dic)


            elif 'CMA-abstract' in xsi and have_front is False or xsi==''  or xsi is None:  # 若xsi中有CMA-abstract字样，且没有front节点 说明文件为CMA 老版简要题录型信息
                # 此类型没有引文信息
                pass

            else:
                print('ERROR:xml文件 ' + str(xml_path) + 'YW表未能进行解析分类')

        df_YW = pd.DataFrame(YW_list)
        df_YW=df_YW.replace('None', '')
        df_YW=df_YW.fillna('')

        return df_YW

    def YW_process_on_string_level(self, xml_path):
        with open(xml_path, "rb") as fp:
            ori_bin = fp.read()
            fp.close()
            try:
                xml = ori_bin.decode('utf-8')
            except:
                xml = ori_bin.decode('utf-16')

        reg_replacelist=[('(<front[>])([\t\n\s\S]*?)(<\/front>)','')]
        for reg_replacement in reg_replacelist:
            xml = re.sub(reg_replacement[0] ,reg_replacement[1], xml)
        xml_list=[]

        for line in xml.splitlines():
            xml_list.append(line)
        begin_search_pattern='<ref id[\t\n\s\S]*?>'
        stop_search_pattern='<trans-mixed-citation.*?>'

        all_YW_unit=[]
        single_unit=[]
        stop_append_flag = 0
        for string in xml_list:
            string = string.lstrip()
            if re.search(begin_search_pattern,string) is not None:
                stop_append_flag=0
                ref_string=''.join(single_unit)
                ref_string= re.sub('(<.*?>)', '', ref_string)
                ref_string= re.sub(' +', ' ', ref_string)
                all_YW_unit.append(ref_string)
                single_unit = []
                single_unit.append(string)
            else:
                if re.search(stop_search_pattern,string) is None and stop_append_flag==0:
                    single_unit.append(string)
                elif re.search(stop_search_pattern,string) is not None:
                    stop_append_flag=1
                    continue
                else:
                    continue

        ref_string = ''.join(single_unit)
        ref_string = re.sub('(<.*?>)', '', ref_string)
        ref_string= re.sub(' +', ' ', ref_string)
        all_YW_unit.append(ref_string)
        #for i in range(1,len(all_YW_unit)):
        #    print(all_YW_unit[i])

        return all_YW_unit


if __name__=='__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    xml_path=r'J:\医学会xml\FtpDownload\0253-2352\1004601.xml'
    test_xml_list=[r'J:\医学会xml\FtpDownload\0253-2352\1004601.xml']
    df_test=bached_xml_prase_YW().generate_YW(test_xml_list)
    df_test.to_csv('test.csv')
    #print(df_test)


