#快速操作
##环境需求
运行本程序需要Python3环境并配置相关依赖包, 依赖包离线安装方法见文件夹下”环境配置.txt”

##运行本程序与参数设置
###主程序名称为：TASK_CMA_to_三表_MAIN.py，正常使用时只运行此程序即可

运行前需要设置的参数如下：
 
*xml_path*:  医学会xml的文件夹路径，程序会对其中包含的所有以‘.xml’或’.XML’结尾的文件进行解析
*time_range*:  待解析数据的时间区间，程序会搜索xml文件修改时间在该区间内的文件进行解析，该处需填入[上限，下限] 时间格式为"%Y/%M/%D %h:%m:%s 若上限或下限不设置，请置””。
*batch_size*： 处理批次大小，程序以此数值为上限分批解析xml文件并写入sqlsever，此数值依照内存大小调整
*sever*:   连接的sqlsever地址
*username*:  sqlsever登录名
*password*:  sqlsever登录密码
*database*:  sqlsever连接的数据库名

table_name_LW、table_name_YW、table_name_ZZ:  sqlsever接收数据写入的三表名称（truncate_table=True时，对应表格会重新创建）

table_name_ZZcomb: 异常论文表多值合并修补表，该表用于论文表作者多值字段的订正。具体使用方法见下文“注意”部分。

generate_lw、generate_yw、generate_zz： 是否进行对应表格内容解析，True为生成

truncate_table: 是否重置表格，当设置为True时，程序会按照上面设置的三表名称重新生成表格


    注意!

    在程序完成输出后，论文表作者信息的多值合并字段可能会出现部分合并错误，请使用table_name_ZZcomb字段表对其进行修正，目前已有修正该问题的存储过程：
    [医学会转三表_ZY].[dbo].[proc_ZZ_LW表相互修正]，若结果表生成位置在163服务器[医学会转三表_ZY]，则直接运行该存储过程即可

#程序说明

###总体程序结构说明：
程序按名称开头分为两类，TOOL开头为通用类，在其他xml解析任务中可直接使用。TASK开头为特定任务类，特定任务类涉及xml预处理；解析位置选取；字段值规范、替换、合并、对应关系识别等处理。

医学会转三表任务已整合至TASK_CMA_to_三表_MAIN.py 进行统一运行。
 
    
通用类作用与xml解析原理说明
TOOL_xml_read.py：本类函数主要作用为获取某文件夹下所有xml路径

TOOL_xml_to_ordereddic.py: 本类函数主要作用为对单一xml进行预处理，并转换为ordereddict格式

TOOL_ordereddic_prase.py: 本类函数主要作用为将xml转化的ordereddict解析为[(路径，值)，（路径，值）...]列表，列表中值的次序与该值在原xml文件中位置次序一致

TOOL_prased_path_to_value.py: 本类函数主要作用为获取给定路径（通配路径）下对应值，同一路径下存在多值按给定分隔符合并（如有获取对应关系需求，按单元分别解析或合并结果携带行次序进行对应）

TOOL_result_generate.py: 本类函数主要作用为将dataframe输入sqlsever或生成csv平面文件


##程序维护

###程序可通过规则表文件夹下 feature_path_CMA.xlsx 和 feature_check_CMA.xlsx进行简单维护。

####字段取值路径维护：
feature_path_CMA.xlsx表格用于控制字段获取的路径，数据示例如下：
 
由于医学会xml同一字段可能存在多种路径，xml位置实际使用了通配方法，以DOI为例：路径/article/front/article-meta/article-id[@pub-id-type=doi]/#text 在获取对应值时，‘/’会被转为通配符‘*’以匹配任何任意长度的字符，故上述路径并非严格路径而是必须出现的节点名称。而对于路径中不允许出现的节点，需写入[xml路径过滤]列，多个节点间使用英文分号分隔。
对于xml位置为空的字段，取值也将置空。
不同类别的表，取值路径保存在不同sheet中。

####输出表维护
feature_check_CMA.xlsx 用于控制输出表的字段与数据类型，数据示例如下
 
    输出过程将按照此表格规则在sqlsever生成表格并填入数据，若输入字段与规则表不一致，多余字段将被舍弃，缺少字段将置空并在运行窗口打印提示。
   
####取值规范与处理
此任务涉及TASK开头的特定任务类，需要修改程序过程，请联系张宇进行处理



##可能的报错原因与处理：
####字符串截断错误：
报错代码如：
pymssql._mssql.MSSQLDatabaseException: (8152, b'String or binary data would be truncated.DB-Lib error message 20018
 
解决办法：
修改规则表文件夹下feature_check_CMA.xlsx，适当增大字符长度限制。

####作者单位对应失败
运行窗口报错如：
ERROR: %82%G:\医学会xml\FtpDownload\0253-3758\1331628.xml英文\中文作者单位信息存在问题
 
本问题主要由于xml文件中作者单位存在缺失导致（有单位序号但无单位名称）
目前对缺失单位采用置空处理

####未能识别的xml类型
运行窗口报错如：
ERROR:xml文件G:\医学会xml\FtpDownload\0253-3758\1331628.xml LW/YW/ZZ表未能进行解析分类

此问题出现由于出现了未归类的xml类型，可能由于改版引起，请联系张宇添加对应类别的处理过程。
