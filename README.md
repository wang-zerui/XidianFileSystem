[📎hadoop-xdfs-1.0.zip](https://www.yuque.com/attachments/yuque/0/2022/zip/1374390/1652711201811-0cf4aa80-fe47-4123-8158-597b836ee3fe.zip)

## 环境

- 环境：windows
- jdk: 1.7 (只试用了1.7)
- Hadoop2.7.3

## 使用步骤

1. 解压缩
2. 将jar包放到`hadoop-2.7.3\share\hadoop\hdfs\lib`下
3. 编辑`hadoop-2.7.3/etc/hadoop/core-site.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>fs.xidian.impl</name>
        <value>org.apache.hadoop.fs.xd.XdFileSystem</value>
    </property>
</configuration>
```

1. 命令行操作测试

测试时保证有对应文件存在

文件路径格式`xidian://`+`路径`，`路径`不包含分卷名（c盘d盘等），注意用斜杠分割不要用`\`

```git
hadoop fs -ls xidian:///test
hadoop fs -mkdir xidian:///test/new_folder
hadoop fs -cp xidian:///test/test.txt xidian://test/data
hadoop fs -cp xidian:///test/test.txt xidian:///test/data
hadoop fs -copyFromLocal file:///tmp/back.txt xidian:///test/data
hadoop fs -cat xidian:///test/test.txt
```

我的目录

![img](https://cdn.nlark.com/yuque/0/2022/png/1374390/1652711523355-a6477375-a5e0-40ad-8e6d-74e755e6ab81.png)

运行结果截图

![img](https://cdn.nlark.com/yuque/0/2022/png/1374390/1652711633592-5fc73dee-b02b-4206-a964-7138ac46a28c.png)
