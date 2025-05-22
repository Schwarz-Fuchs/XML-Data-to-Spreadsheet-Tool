from TASK_CMA_to_三表LW import bached_xml_prase_LW
from TASK_CMA_to_三表YW import bached_xml_prase_YW
from TASK_CMA_to_三表ZZ import bached_xml_prase_ZZ
import pandas as pd
from TOOL_result_generate import batch_prepare, result_to_sqlsever
from TOOL_xml_read import xml_read
import datetime
import pymssql


from TOOL_prased_path_to_value import prased_path_to_value
from sqlalchemy import create_engine

if __name__=='__main__':
    ##在这里输入解析参数
    xml_path=r'J:\医学会xml_new\FtpDownload'  ##在这里输入需要解析的包含xml的文件夹
    time_range=["", ""] ##在这里输入需要解析的文件修改时间区间[上限，下限] 时间格式为"%Y/%M/%D %h:%m:%s 若上限或下限不设置，请置''
    #xml_path=r'G:\医学会xml\FtpDownload\debug2'
    batch_size = 50000
    sever='10.192.64.163'                       ##服务器地址
    user_name='sa'                              ##MSSQL数据库用户名
    password='Devdb.@163'                         ##MSSQL用户密码
    database='医学会转三表_ZY'             ##数据转出的目标数据库名称（需要提前创建）
    table_name_LW='三表_LW_CMA'            ##数据转出的目标表 论文表
    table_name_YW='三表_YW_CMA'            ##数据转出的目标表 引文表
    table_name_ZZ='三表_ZZ_CMA'            ##数据转出的目标表 作者表
    table_name_ZZcomb='三表_ZZcomb_CMA'

    generate_lw=True                    ##是否进行论文表提取
    generate_yw=True                      ##是否进行引文表提取
    generate_zz=True                     ##是否进行作者表提取
    truncate_table=True                   ##每次运行是否重新生成表格 (报错继续运行时请改为False)

    fail_data_continue=False              ##是否从batch断点继续运行（报错修正后使用，继续运行未完成的batch）

    '''
    #要输出scv时
    result_path='结果表'  #生成csv的地址
    '''

    connect_db = pymssql.connect(sever, user_name, password, database)
    db_cursor = connect_db.cursor()

    if fail_data_continue==False:
        all_file_path=[]
        print('正在获取目录下所有的xml路径......')
        xml_all=xml_read().findxml_lit_time_fliter(xml_path,all_file_path,time_range)
        batch_file_path=batch_prepare().split_batch(all_file_path,batch_size)
        print('正在初始化表格......',end=' ')

        # 初始化LOG表
        result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', 'LOG_table', user_name, password, sever,
                                              database, '数据写入记录')
        path_dic_list = []
        for path in all_file_path:
            path_dic_list.append({'filepath': path, '状态': '未执行', '完成时间': ''})
        df_path_log = pd.DataFrame(path_dic_list)
        result_to_sqlsever().data_insert(df_path_log, user_name, password, sever, database, '数据写入记录')
        sql_string_1 = "create index filepath on " + str(database) + ".dbo.数据写入记录(filepath)"
        sql_string_0 = "create index [状态] on " + str(database) + ".dbo.数据写入记录([状态])"
        db_cursor.execute(sql_string_1)
        db_cursor.execute(sql_string_0)
        connect_db.commit()

        # 初始化表格 sqlsever 输出
        if generate_lw == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_LW', user_name, password, sever,
                                                  database, table_name_LW)
        if generate_zz == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_ZZ', user_name, password, sever,
                                                  database, table_name_ZZ)
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_ZZcomb', user_name, password, sever,
                                                  database, table_name_ZZcomb)
        if generate_yw == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_YW', user_name, password, sever,
                                                  database, table_name_YW)

    else:
        all_file_path=[]
        print('准备从断点继续运行...',end='')

        #默认查找未完成记录
        db_cursor.execute("select filepath from "+str(database)+".dbo.数据写入记录 where [状态]!='已执行'")  # sentence为sql指令

        ##可以在这里写入自定义查询
        #db_cursor.execute("select file_path from " + str(database) + ".dbo.LW_prob")
        ##
        if generate_lw == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_LW', user_name, password, sever,
                                                  database, table_name_LW)
        if generate_zz == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_ZZ', user_name, password, sever,
                                                  database, table_name_ZZ)
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_ZZcomb', user_name, password, sever,
                                                  database, table_name_ZZcomb)
        if generate_yw == True and truncate_table == True:
            result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', '三表_YW', user_name, password, sever,
                                                  database, table_name_YW)

        result = db_cursor.fetchall()
        connect_db.commit()
        for item in result:
            all_file_path.append(item[0])
        batch_file_path = batch_prepare().split_batch(all_file_path, batch_size)

    '''
    #初始化表格_csv
    result_generate_csv().table_initialize('结果表')
    '''
    print('done')
    print('*****************************')

    #生成表格
    for i in range(len(batch_file_path)):
        print('当前正在处理第'+str(i+1)+'批，所有数据共'+str(len(batch_file_path))+'批。')
        print('*****************************')
        #分别生成三表
        if generate_lw == True:
            print('生成论文表...')
            df_LW=bached_xml_prase_LW().generate_LW(batch_file_path[i])
            df_LW_checked=batch_prepare().column_check('规则表/feature_check_CMA.xlsx', '三表_LW', df_LW)
            result_to_sqlsever().data_insert(df_LW_checked, user_name, password, sever, database, table_name_LW)
            print('论文表完成写入')

        if generate_zz == True:
            print('生成作者表...')
            df_ZZ=bached_xml_prase_ZZ().generate_ZZ(batch_file_path[i])
            df_ZZ_checked=batch_prepare().column_check('规则表/feature_check_CMA.xlsx', '三表_ZZ', df_ZZ)
            df_ZZ_comb = bached_xml_prase_ZZ().generate_ZZ_comtable(df_ZZ_checked)
            result_to_sqlsever().data_insert(df_ZZ_checked, user_name, password, sever, database, table_name_ZZ)
            result_to_sqlsever().data_insert(df_ZZ_comb, user_name, password, sever, database, table_name_ZZcomb)
            print('作者表完成写入')

        if generate_yw == True:
            print('生成引文表...')
            df_YW=bached_xml_prase_YW().generate_YW(batch_file_path[i])
            df_YW_checked=batch_prepare().column_check('规则表/feature_check_CMA.xlsx', '三表_YW', df_YW)
            result_to_sqlsever().data_insert(df_YW_checked, user_name, password, sever, database, table_name_YW)
            print('引文表完成写入')


        #更新LOG
        time=datetime.datetime.now()
        result_to_sqlsever().table_initialize('规则表/feature_check_CMA.xlsx', 'LOG_table', user_name, password, sever,
                                              database, '数据写入记录_单批次')
        path_dic_list = []
        for path in all_file_path:
            path_dic_list.append({'filepath': path, '状态': '已执行', '完成时间': str(time)})
        df_path_log = pd.DataFrame(path_dic_list)
        result_to_sqlsever().data_insert(df_path_log, user_name, password, sever, database, '数据写入记录_单批次')
        sql_string_2= "create index filepath on "+str(database)+".dbo.数据写入记录_单批次(filepath)"
        db_cursor.execute(sql_string_2)
        sql_string_3="update a set a.状态=b.状态,a.完成时间=b.完成时间 from "+str(database)\
                     +".dbo.数据写入记录 a inner join "+str(database)+".dbo.数据写入记录_单批次 b on a.filepath=b.filepath"
        db_cursor.execute(sql_string_3)
        connect_db.commit()
        #三表字段核查
        #8152错误：截断错误

    print('***********************')
    connect_db.close()
    print('提取过程结束，程序已退出')



    #以下为debug测试代码 ，需要注释掉， 不在程序中执行
    ##单xml结构查看
    '''
    xml_path=r'F:\医学会XML\FtpDownload\9999-999X\1368787.xml'
    replace_list = [['\n', '']]
    reg_list = []
    xml_dict = xml_to_ordereddic().xml_to_dic_standardize(xml_path, replace_list, reg_list)
    # 解析全部分支
    ignore_list = ['@xmlns:mml', '@xmlns:xlink', '@xmlns:xsi', '@dtd-version', '@xsi:noNamespaceSchemaLocation'
        , '@article-type']  # 不进入路径的修饰符
    stop_list = ['graphic','journal-meta','article-categories','title-group','contrib-group','trans-contrib-group'
                 ,'contrib-group','author-notes','pub-date','page-range','history','permissions','self-uri',
                 'abstract','kwd-group','funding-group','counts']  # 不进行解析的节点
    main_list = ordereddic_prase().extract_main_branch(xml_dict, xml_path, ignore_list, stop_list)
    for item in main_list:
        print(item)

    feature_value = prased_path_to_value().get_value_lit(main_list, '/article[@xml:lang=zh]/front/article-meta/abstract', '')
    print(feature_value)
    '''
    #bached_xml_prase().generate_YW([r'F:\医学会XML\FtpDownload\9999-999X\1368787.xml'])
