import xmltodict
import re

class xml_to_ordereddic:

    def __init__(self):
        pass

    def xml_to_dic(self ,filepath):
        '''
        调包，xmltodict.parse(xml)将xml解析成ordereddict和list嵌套的数据格式(类似json格式）
        此方法不对原xml进行任何修改，直接转换为类json的数据格式
        :param filepath: xml文件路径
        :return: 解析后的 ordereddict和list嵌套的数据格式
        '''
        with open(filepath, "rb") as fp:
            ori_bin = fp.read()
            fp.close()
            try:
                xml = ori_bin.decode('utf-8')
            except:
                xml = ori_bin.decode('utf-16')
            get_xml = xmltodict.parse(xml)
        return get_xml

    def xml_to_dic_standardize(self ,filepath ,replacelist ,reg_replacelist):
        '''
        调包，xmltodict.parse(xml)将xml解析成ordereddict和list嵌套的数据格式(类似json格式）
        此方法通过replace 和
        :param filepath: xml文件路径
        :param replacelist:  使用replace方法匹配字符串并修改 ；list应有格式 [['<待替换字符串>','<替换字符串>']，['<待替换字符串>','<替换字符串>'],...]  注意替换是按次序进行的
                       例：[['<Sub>', ''],['</Sub>', ''],['<I>', ''],['</I>', '']]
        :param reg_replacelist: 使用正则表达式方法匹配字符串并修改； list应有格式  [['<正则式>','<替换字符串>'],['<正则式>','<替换字符串>']...] 注意替换是按次序进行的
                          例：[['<U .*">',''],['[A-Z]','X'],...]
        :return: 解析后的 ordereddict和list嵌套的数据格式

        '''
        with open(filepath, "rb") as fp:
            ori_bin = fp.read()
            fp.close()
            try:
                xml = ori_bin.decode('utf-8')
            except:
                xml = ori_bin.decode('utf-16')

            # 使用字符replace方法替换原xml内容
            for replacement in replacelist:
                xml = xml.replace(replacement[0] ,replacement[1])
            # 使用正则式方法替换原xml内容
            for reg_replacement in reg_replacelist:
                xml = re.sub(reg_replacement[0] ,reg_replacement[1], xml)
            get_xml = xmltodict.parse(xml)
        return get_xml
