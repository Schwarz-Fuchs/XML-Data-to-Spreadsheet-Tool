# 进行任务执行时，请引入下列工具
from TOOL_xml_read import *
from TOOL_xml_to_ordereddic import *
from TOOL_ordereddic_prase import *
from TOOL_result_generate import *
from tqdm import tqdm
from fnmatch import fnmatch
import base64

import copy
class bached_xml_prase_LW:

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

    def generate_LW(self,batch_xml_list):

        LW_list=[]
        feature_list_JAST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'JAST_三表_LW')
        feature_list_ABST = prased_path_to_value().read_feature('规则表/feature_path_CMA.xlsx', 'ABST_三表_LW')

        for xml_path in tqdm(batch_xml_list):
            replace_list = [['\n', ''],['\r', ''],['      ',''],['<sub>',''],['</sub>',''],['<sup>',''],['</sup>',''],
                            ['<italic>',''],['</italic>','']]
            reg_list = [[' +', ' ']]
            xml_dict = xml_to_ordereddic().xml_to_dic_standardize(xml_path, replace_list, reg_list)


            #解析全部分支
            ignore_list = ['@xmlns:mml', '@xmlns:xlink', '@xmlns:xsi', '@dtd-version', '@xsi:noNamespaceSchemaLocation'
                , '@article-type']  #不进入路径的修饰符
            stop_list = ['graphic','back','permissions']  #不进行解析的节点
            main_list = ordereddic_prase().extract_main_branch(xml_dict, xml_path, ignore_list, stop_list)
            #区分JAST 和 abstract 的两节点
            xsi=prased_path_to_value().get_value_lit(main_list,'/article/@xsi:noNamespaceSchemaLocation','')
            have_front=prased_path_to_value().have_node(main_list,'front')

            if 'CMA-article' in xsi or have_front is True:  #若xsi中有CMA-article字样或有front节点，说明文件为CMA JAST 型题录信息 或新版简要题录型数据
                extract_feature=[]
                ##规则表待补充
                for feature_and_path in feature_list_JAST:
                    feature_name=feature_and_path[0]
                    feature_path=feature_and_path[1]
                    feature_path_fliter=feature_and_path[2]
                    feature_value=prased_path_to_value().get_value_lit(main_list,feature_path,feature_path_fliter)
                    extract_feature.append((feature_name,feature_value))
                all_feature_dic=dict(extract_feature)

                for key, value in all_feature_dic.items():
                    if value=='None':
                        all_feature_dic.update({key:''})

                #print (all_feature_dic)

                ##在此处对dic内字段进行合并，规范等操作
                ##1.合并日期并删除原字段
                CB=''
                XG=''
                SG=''
                if all_feature_dic['CB_year'] is not None and all_feature_dic['CB_month'] is not None and all_feature_dic['CB_day'] is not None and all_feature_dic['CB_year'] !='' and all_feature_dic['CB_month'] !='' and all_feature_dic['CB_day'] !='':
                    CB=str(all_feature_dic['CB_year'])+'-'+str(all_feature_dic['CB_month'])+'-'+str(all_feature_dic['CB_day'])
                if all_feature_dic['XG_year'] is not None and all_feature_dic['XG_month'] is not None and all_feature_dic['XG_day'] is not None and all_feature_dic['XG_year'] !='' and all_feature_dic['XG_month'] !='' and all_feature_dic['XG_day'] !='':
                    XG=str(all_feature_dic['XG_year'])+'-'+str(all_feature_dic['XG_month'])+'-'+str(all_feature_dic['XG_day'])
                if all_feature_dic['SG_year'] is not None and all_feature_dic['SG_month'] is not None and all_feature_dic['SG_day'] is not None and all_feature_dic['SG_year'] !='' and all_feature_dic['SG_month'] !='' and all_feature_dic['SG_day'] !='':
                    XG=str(all_feature_dic['SG_year'])+'-'+str(all_feature_dic['SG_month'])+'-'+str(all_feature_dic['SG_day'])
                all_feature_dic.update({'CB':CB})
                all_feature_dic.update({'XG':XG})
                all_feature_dic.update({'SG':SG})
                del all_feature_dic['CB_year'];del all_feature_dic['CB_month'];del all_feature_dic['CB_day']
                del all_feature_dic['XG_year'];del all_feature_dic['XG_month'];del all_feature_dic['XG_day']
                del all_feature_dic['SG_year'];del all_feature_dic['SG_month'];del all_feature_dic['SG_day']
                ##2.选择单位并删除原字段
                DWys=''
                if all_feature_dic['DWys_pattern2'] != '' and str(all_feature_dic['DWys_pattern2']) is not None:
                    DWys=all_feature_dic['DWys_pattern2']
                elif  all_feature_dic['DWys_pattern1'] != '' and str(all_feature_dic['DWys_pattern1']) is not None:
                    DWys=all_feature_dic['DWys_pattern1']
                elif  all_feature_dic['DWys_en_pattern2'] != '' and str(all_feature_dic['DWys_en_pattern2']) is not None:
                    DWys=all_feature_dic['DWys_en_pattern2']
                elif  all_feature_dic['DWys_en_pattern1'] != '' and str(all_feature_dic['DWys_en_pattern1']) is not None:
                    DWys=all_feature_dic['DWys_en_pattern1']

                all_feature_dic.update({'DWys': DWys})
                del all_feature_dic['DWys_pattern1'];del all_feature_dic['DWys_pattern2'];del all_feature_dic['DWys_en_pattern2'];del all_feature_dic['DWys_en_pattern1']
                ##3.英文作者姓名处理  更安全的方法在ZZ表中，不过更慢
                ZZE = ''
                if all_feature_dic['ZZE_sur'] != '' and  all_feature_dic['ZZE_sur'] is not None and all_feature_dic['ZZE_given'] != '' and all_feature_dic['ZZE_given'] is not None:
                    sur_list=all_feature_dic['ZZE_sur'].split(';')
                    given_list=all_feature_dic['ZZE_given'].split(';')
                    if len(sur_list)==len(given_list):
                        for i in range(len(sur_list)):
                            ZZE=ZZE+str(sur_list[i])+' '+str(given_list[i])+';'
                        ZZE=ZZE[:-1]
                all_feature_dic.update({'ZZE': ZZE})

                #如果中文作者为空，使用英文作者填充
                if all_feature_dic['ZZ'] =='' or all_feature_dic['ZZ'] is None:
                    all_feature_dic.update({'ZZ': ZZE})

                del all_feature_dic['ZZE_sur'];del all_feature_dic['ZZE_given']
                ##4.基金项目
                jjbh_list=all_feature_dic['JJbh'].split(';')
                jjxm=all_feature_dic['JJxm']
                for jjbh in jjbh_list:
                    jjxm=jjxm.replace(';'+str(jjbh),'('+str(jjbh)+')')
                all_feature_dic.update({'JJxm':jjxm})
                ##5.摘要
                ZY=all_feature_dic['ZY'].replace(';',' ')
                ZYE=all_feature_dic['ZYE'].replace(';',' ')
                all_feature_dic.update({'ZY': ZY})
                all_feature_dic.update({'ZYE': ZYE})

                ##6.老版刊物出版年
                if all_feature_dic['N'] =='' or all_feature_dic['N'] is None:
                    if all_feature_dic['pubdate']!='' and all_feature_dic['pubdate'] is not None:
                        date_list=all_feature_dic['pubdate'].split('-')
                        if len(date_list)>1:
                            year=date_list[0]
                            all_feature_dic.update({'N':year})

                if all_feature_dic['N'] =='' or all_feature_dic['N'] is None:
                    if all_feature_dic['N_epub']!='' and all_feature_dic['N_epub'] is not None:
                        all_feature_dic.update({'N':all_feature_dic['N_epub']})

                ##7.生成 QCODE,使用后两位路径的base64码
                all_feature_dic.update({'QCODE': self.base64_encode(str(all_feature_dic['file_path']))})

                ##8.页码补充 使用起始页到终止页补充
                if all_feature_dic['Y']=='' or all_feature_dic['Y'] is None :
                    start_page=all_feature_dic['Fpage']
                    end_page = all_feature_dic['Lpage']
                    if start_page == '' or start_page is None:
                        start_page = all_feature_dic['Fpage_b']
                    if end_page == '' or end_page is None:
                        end_page = all_feature_dic['Lpage_b']
                    if end_page!=start_page:
                        page_range=str(start_page)+'-'+end_page
                    else:
                        page_range=start_page
                    all_feature_dic.update({'Y':page_range})

                ##9. 分类号备用值
                if all_feature_dic['FL']=='' or all_feature_dic['FL'] is None :
                    all_feature_dic.update({'FL': all_feature_dic['FL_bk']})

                ##dic内字段规范结束
                LW_list.append(all_feature_dic)

            elif ('CMA-abstract' in xsi and have_front is False) or xsi==''  or xsi is None:  #若xsi中有CMA-abstract字样，且没有front节点 说明文件为CMA 老版简要题录型信息
                extract_feature = []
                ##规则表待补充
                for feature_and_path in feature_list_ABST:
                    feature_name = feature_and_path[0]
                    feature_path = feature_and_path[1]
                    feature_path_fliter = feature_and_path[2]
                    feature_value = prased_path_to_value().get_value_lit(main_list, feature_path, feature_path_fliter)
                    #此形式下多值分隔符需规范
                    if  feature_value != '' and  feature_value is not None:
                        feature_value=feature_value.replace('|',';')
                    extract_feature.append((feature_name, feature_value))
                all_feature_dic = dict(extract_feature)

                for key, value in all_feature_dic.items():
                    if value=='None':
                        all_feature_dic.update({key:''})

                ##在此处对dic内字段进行合并，规范等操作
                ##1.年为空，使用pdate替换
                if all_feature_dic['N']=='' or all_feature_dic['N'] is None:
                    if all_feature_dic['pubdate']!='' and all_feature_dic['pubdate'] is not None:
                        date_list=all_feature_dic['pubdate'].split('-')
                        if len(date_list)>2:
                            all_feature_dic.update({'N':date_list[0]})

                ##2.生成 QCODE,使用后两位路径的base64码
                all_feature_dic.update({'QCODE': self.base64_encode(str(all_feature_dic['file_path']))})

                ##2.分类号分隔符修正
                all_feature_dic.update({'FL': str(all_feature_dic['FL'].replace('|',';'))})

                ##8.页码补充 使用起始页到终止页补充
                if all_feature_dic['Y'] == '' or all_feature_dic['Y'] is None:
                    start_page = all_feature_dic['Fpage']
                    end_page = all_feature_dic['Lpage']
                    if start_page=='' or start_page is None:
                        start_page= all_feature_dic['Fpage_b']
                    if end_page=='' or end_page is None:
                        end_page=all_feature_dic['Lpage_b']
                    if end_page != start_page:
                        page_range = str(start_page) + '-' + end_page
                    else:
                        page_range = start_page
                    all_feature_dic.update({'Y': page_range})

                ##dic内字段规范结束
                LW_list.append(all_feature_dic)

            else:
                 print('ERROR:xml文件 '+str(xml_path)+'LW表未能进行解析分类')

        df_LW = pd.DataFrame(LW_list)
        df_LW=df_LW.replace('None', '')
        df_LW=df_LW.fillna('')
        return df_LW








