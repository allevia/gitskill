建立项目名称为Matrix，在Matrix的src中加入该代码。

我运行该代码时文件结构如下：
hdfs://localhost:9000/user/user/inputm中存放矩阵文件 M_10_15和N_15_20；
输出路径为 hdfs://localhost:9000/user/user/outputm；

程序将eclipse与hadoop用自己编译的插件连接后运行，在configuraion的arguments中program argument栏输入两矩阵文件的输入路径及输出路径即可运行。