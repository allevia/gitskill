全部代码分为三个大类：数据初步处理、朴素贝叶斯及KNN
运行时先运行数据初步处理类（Data_proc.java）,输入参数顺序为：训练集数据路径、数据初步处理输出路径、停词表路径；
然后运行朴素贝叶斯相关类（Search_Data.java, Proc_prob.java和NaiveB.java，主类为Search_Data.java），输入参数顺序为：测试集数据输入路径、测试数据处理后结果输出路径、停词文件路径(用于Search_Data.java)、预测结果输出路径（用于NaiveB.java）、数据初步处理输出路径及根据初步处理结果得到的概率输出路径（用于Proc_prob.java）；
最后运行KNN相关类（ITF.java, TF_idf_test.java, Tf_idf_train.java, KNN.java及Instance.java）分为三大块。ITF.java和Tf_idf_train.java计算训练数据TF-TDF向量，主类为Tf_idf_train.java；输入参数顺序为：数据初步处理阶段输出路径、计算ITF的单独输出路径、初步数据处理阶段基本情况统计输出路径、训练数据输入路径、训练数据tf-idf值输出路径及停词文件路径；TF_idf_test.java计算测试数据TF-IDF向量，输入参数顺序为：测试数据输入路径、测试数据tf值输出路径、停词文件路径及最终tf-idf值输出路径； KNN.java及Instance.java最终计算预测结果，主类为KNN.java，输入参数顺序为：测试数据tf-idf值输出路径、最终结果输出路径、训练集tf-idf值文件路径及所用KNN的k值

由于参数较多比较复杂，可以参见实验报告对输入参数的示例；
为更好了解代码功能，可以参见实验报告，还可以结合/辅助数据文件/中间结果文件进一步了解。
