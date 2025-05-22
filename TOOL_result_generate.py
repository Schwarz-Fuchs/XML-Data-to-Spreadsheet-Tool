import pandas as pd
import pymssql
from TOOL_prased_path_to_value import prased_path_to_value
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

class batch_prepare:
    def __init__(self):
        pass
    def split_batch(self, init_list, batch_size):
        '''
        :param init_list:   总表格
        :param batch_size:  每批次数据数量
        :return: 将init_list中数据按批次切分的list结果  ([10000]-->[[1000],[1000]...[1000]])
        '''
        groups = zip(*(iter(init_list),) * batch_size)
        end_list = [list(i) for i in groups]
        count = len(init_list) % batch_size
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list

    def column_check(self,check_excel_path, sheet_name, check_dataframe):
        '''
        将生成的dataframe规范为需要的字段形式，剔除多余字段，提示并生成未能查找到的字段
        :param check_excel_path:
        :param sheet_name:
        :param csv_path:
        :return:
        '''
        df = check_dataframe
        data = pd.read_excel(check_excel_path, sheet_name=sheet_name, engine='openpyxl', keep_default_na=False)
        column_name = []
        for item in data.values[:, 0]:
            if item != '':
                column_name.append(item)

        lacking_list = []
        for item in column_name:  #####所有输入SQL的表都需要经过exam!!!  如果表的字段多于SQL预设字段会导致无法插入！！！
            if item not in df.columns:
                lacking_list.append(item)
        if len(lacking_list) > 0:
            print('以下字段未被找到,已使用空值填充:\n', lacking_list)
            df[lacking_list] = ''
        else:
            print('未发现缺少字段，将执行数据输出')
        new_df = df[column_name]

        return new_df





class result_generate_csv:
    def __init__(self):
        pass

    def table_initialize(self,check_excel_path,check_excel_sheet,target_csv_path):
        feature=prased_path_to_value().read_feature(check_excel_path,check_excel_sheet)
        #初始化三表，对字段进行规定
        feature_list=[]
        for item in feature:
            feature_list.append(item[0])

        df_LW = pd.DataFrame(columns=feature_list)
        df_LW.to_csv(str(target_csv_path))

    def csv_append(self,csv_path,new_dataframe):
        '''
        把新的dataframe部分追加写入csv
        :param csv_path:  需要追加写入的csv路径
        :param new_dataframe:  增量dataframe
        :return:
        '''
        df_previous = pd.read_csv(csv_path,dtype=object)
        df_main_new=pd.concat([df_previous,new_dataframe])
        df_main_new.to_csv(csv_path,index=False)


class result_to_sqlsever:
    def __init__(self):
        pass

    def table_initialize(self,check_excel_path,check_excel_sheet,user_name,password,sever,database_name,target_table_name):
        try:
           connect_db = pymssql.connect(sever, user_name, password, database_name)
           flag=1
        except:
           flag=0
           print('ERROR:初始化表格时，数据库连接失败，请检查输入参数')
        if flag==1:
            features= prased_path_to_value().read_feature(check_excel_path, check_excel_sheet)
            feature_list=[]
            datakind_list=[]
            for item in features:
                feature_list.append(item[0])
            for item in features:
                datakind_list.append(item[1])

            db_cursor = connect_db.cursor()
            ##在目标库生成新表
            if len(feature_list)==len(datakind_list):
                #删除已有表格
                sql_string_1="IF OBJECT_ID('["+str(database_name)+"].[dbo]."+"["+target_table_name+"]',N'U') IS NOT NULL  DROP TABLE ["+str(database_name)+"].[dbo]."+"["+target_table_name+"]"
                db_cursor.execute(sql_string_1)

                #生成表格
                sql_string_2="create table ["+str(database_name)+"].[dbo]."+"["+target_table_name+"] ("
                for i in range(len(feature_list)):
                    sql_string_2=sql_string_2+"["+str(feature_list[i])+"] "+ str(datakind_list[i])+","
                sql_string_2=sql_string_2[:-1]+")"
                db_cursor.execute(sql_string_2)

                connect_db.commit()
                connect_db.close()
            else:
                 print('ERROR: 核查表字段与数据类型未能正确对应，表格初始化失败')



    def data_insert(self,dataframe,user_name,password,sever,database_name,target_table_name,replace=False):

        password = urlquote(password)
        conn_comamd='mssql+pymssql://'+str(user_name)+':'+str(password)+'@'+str(sever)+'/'+str(database_name)
        try:
           conn = create_engine(conn_comamd)
           flag=1
        except:
           flag=0
           print('ERROR:输入数据时，数据库连接失败，请检查输入参数')

        if replace==True:
            replace='replace'
        else:
            replace='append'
        if flag==1:
            dataframe.to_sql(name=target_table_name, con=conn, if_exists=replace, index=False)








##在这里运行测试
if __name__=='__main__':
    result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx','三表_YW','sa','sa','(local)','医学会转三表_测试','输入测试')





