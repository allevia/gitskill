建立项目名称为relation，在relation的src中加入该代码。

我运行此代码时文件结构如下：
hdfs://localhost:9000/user/user/inputr中存放处理文件Ra.txt；
输出路径为 hdfs://localhost:9000/user/user/outputr/selection；

设置选择条件时，如果要按大于选择，将greater置为true,否则为false；

程序将eclipse与hadoop用自己编译的插件连接后运行，在configuraion的arguments中program argument栏依次输入关系文件所在路径、输出路径、参与选择的列标号（从0至3）以及用于判断的值，VM arguments中输入系统变量greater的值。