建立项目名称为relation，在relation的src中加入该代码。

我运行此代码时文件结构如下：
hdfs://localhost:9000/user/user/inputr1中存放处理文件Ra1.txt和Ra2.txt；
输出路径为 hdfs://localhost:9000/user/user/outputr/difference；

程序将eclipse与hadoop用自己编译的插件连接后运行，在configuraion的arguments中program argument栏依次输入关系文件所在路径，输出路径及被减文件。