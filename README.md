# 工业互联网大数据安全监测：操作序列高频检测模块 
~~~
工业互联网大数据安全监测平台 - Interval Error Detect插件

该插件可以检测工业互联网大数据中的高频队列

该插件提供3种模式：学习模式 / 检测模式 / 清理模式

当前版本：0.3.1 (build 21)
~~~

自动编译信息：[![Build Status](https://www.travis-ci.org/SugarGuan/IntervalErrorDetect.svg?branch=master)](https://www.travis-ci.org/SugarGuan/IntervalErrorDetect)     


## 学习模式

学习模式下，机器获取elasticsearch中保存的工业互联网操作码数据
具体使用了spark操作es，取回PairRDD，通过对不同字段（同一机器组不同操作队列）的统计确定高频子队列，并将具体的操作队列记录在本地（模式）。


## 检测模式

检测模式中，根据学习模式习得（或已有的）模式开展检测。如果检测到高频模式发出报警，向redis发送警报。


## 清理模式

清理模式将清空已有的学习文件（模式）。清理模式结束后返回前一线程模式。

## Future Versions:

1. 改进统计学习算法，降低时间复杂度

2. 考虑引入更多的算法，包括机器学习算法。重点考虑是否能够通过修改数据源的形式，调用`FPGrowth`

3. 考虑设计机器学习模型，习得非连续的高频操作队列
`如：ABCDABC 和 ABCEABC 中 学得 ABC*ABC`


&copy;  2019 Harbin Institute and Technology.
