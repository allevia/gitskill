本次实验任务通过使用自行编译的hadoop-eclipse-plugin-2.9.1插件在eclipse下运行；
实验hadoop环境为2.9.1，eclipse为Release 4.8.0 (Photon)，运行方式为伪分布。

建立项目名称为Kmeans，在relation的src中加入该代码。

其中Instance.java定义节点呈现方式（坐标形式）及节点相关常用操作如读入、加、乘、输出等；
Cluster.java 定义了聚类簇的结构：聚类簇由id、聚类簇中节点个数、中心节点实例组成。同时定义了一些聚类簇常用的操作如获取聚类簇id、节点个数、输出等；
EuclideanDistance.java定义了计算两点间距离的方法；
KMeans.java实现为节点选择中心，输出每次迭代产生的聚类簇；
KMeansCluster.java根据KMeans每次选定的中心及节点属于聚类簇的情况产生每次聚类后的具体情形；
RandomClusterGenerator.java用于初始化数据，从初始数据中随机选取要求个数的中心。
KMeansDriver.java全局统筹整个程序运行，实现KMeans算法的多次迭代。

我运行此代码时文件结构如下：
hdfs://localhost:9000/user/user/KMeansinput中存放处理文件Instance.txt；
输出路径为 hdfs://localhost:9000/user/user/KMeansoutput；

程序将eclipse与hadoop用自己编译的插件连接后运行，在configuraion的arguments中program argument栏依次输入中心个数、迭代次数、关系文件所在路径及输出路径即可运行。

本例中我选取的中心点个数为三，迭代次数为五，将可视化结果展示在文件夹中，可以看到，随迭代次数增加聚类效果更加明显且趋于稳定。
