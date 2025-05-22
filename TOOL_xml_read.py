import os
from tqdm import tqdm
import time

class xml_read:

    def __init__(self):
        pass

    def findxml_lit(self, path, result_list):
        '''
        获取·给定path下所有xml文件
        :param path:  包含xml的路径
        :param result_list:  初始传入表，查找到的xml路径将append到此表中
        :return: 查找结果list
        '''
        filelist = os.listdir(path)
        #print(len(filelist))
        for filename in filelist:
            de_path = os.path.join(path, filename)
            if os.path.isfile(de_path):
                if de_path.endswith(".XML") or de_path.endswith(".xml"):  # Specify to find the txt file.
                    result_list.append(de_path)
            else:
                self.findxml_lit(de_path, result_list)

    def findxml_lit_time_fliter(self, path, result_list,time_range_list):
        '''
        获取·给定path下所有xml文件
        :param path:  包含xml的路径
        :param result_list:  初始传入表，查找到的xml路径将append到此表中
        :return: 查找结果list
        :time_range: 要查找的文件修改区间(list格式)  格式需满足如["2021/4/25 01:00:00", "2021/4/26 02:00:00"] 上下限不设置时置 “”
        '''
        filelist = os.listdir(path)
        #print(len(filelist))
        for filename in filelist:
            de_path = os.path.join(path, filename)
            if os.path.isfile(de_path):
                if de_path.endswith(".XML") or de_path.endswith(".xml"):  # Specify to find the txt file.
                    mtime = os.path.getmtime(de_path)
                    #print(time.ctime(mtime))
                    time_range = time_range_list
                    if time_range[0] == '':
                        time_range[0] = "1972/1/1 02:00:00"
                    if time_range[1] == '':
                        time_range[1] = "2300/1/1 02:00:00"
                    lower_timeStamp = int(time.mktime(time.strptime(time_range[0], "%Y/%m/%d %H:%M:%S")))
                    upper_timeStamp = int(time.mktime(time.strptime(time_range[1], "%Y/%m/%d %H:%M:%S")))
                    if lower_timeStamp <= mtime <= upper_timeStamp:
                        result_list.append(de_path)
            else:
                self.findxml_lit_time_fliter(de_path, result_list,time_range_list)

    def findxml_lit_fliter(self, path, result_list, fliter_list):
        '''
        获取给定path下所有xml文件,同时筛除含有fliter_list中字符的路径文件
        :param path:  包含xml的路径
        :param result_list: 初始传入表，查找到的xml路径将append到此表中
        :param fliter_list:  不允许出现在查找结果路径中的字符，含有此类字符的路径不会进入结果表
        :return:
        '''
        filelist = os.listdir(path)
        for filename in filelist:
            de_path = os.path.join(path, filename)
            if os.path.isfile(de_path):
                if de_path.endswith(".XML") or de_path.endswith(".xml"):  # Specify to find the txt file.
                    accept_flag = 1
                    for fliter_str in fliter_list:
                        if fliter_str in str(de_path):
                            accept_flag = 0
                    if accept_flag == 1:
                        result_list.append(de_path)
            else:
                self.findxml_lit_fliter(de_path, result_list , fliter_list)

if __name__=='__main__':
    file_dir = r'J:\医学会xml\FtpDownload'
    result_list=[]
    xml_read().findxml_lit_time_fliter(file_dir,result_list,["2022/1/1 00:00:00", ""])
    for i in range(100):
        print(result_list[i])







