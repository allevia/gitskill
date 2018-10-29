建立项目名称为wc3，在wc3的src中加入该代码。

我运行此代码时文件结构如下：
hdfs://localhost:9000/user/user/inputw中存放处理文件 I_Have_a_Dream.txt；
hdfs://localhost:9000/user/user 下放要跳过的字符文件patterns.txt；
输出路径为 hdfs://localhost:9000/user/user/outputw；

设置大小写敏感时 将wordcount.case.sensitive置为true,否则为false；

程序将eclipse与hadoop用自己编译的插件连接后运行，在configuraion的arguments中program argument栏输入输入输出路径及patterns.txt的位置，VM arguments中输入系统变量wordcount.case.sensitive的值。