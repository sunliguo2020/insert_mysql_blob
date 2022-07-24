# 在mysql中保存文件
功能说明：

文件的内容类型保存为blob，同时保存文件名和最后的修改时间。
为了去重，添加文件的MD5值作为一列。同时用MD5值来验证blob列保存的文件是否完整。

### 用到的python第三方模块

1、dbutils

DBUtils 是一套用于管理数据库连接池的Python包，为高频度高并发的数据库访问提供更好的性能，可以自动管理连接对象的创建和释放。**并允许对非线程安全的数据库接口进行线程安全包装。**

### 操作步骤：

#### 1、手动创建mysql数据表，默认的数据库名为crawl。

```sql
CREATE TABLE `table_name` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `file_name` char(50) NOT NULL,
  `md5sum` char(32) NOT NULL,
  `blob` mediumblob NOT NULL,
  `mod_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `md` (`md5sum`) USING HASH,
  KEY `fn` (`file_name`) USING HASH
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
```
#### 2、修改py文件中的要导入的文件的目录和数据表名。

```python
  # 导入文件所在的目录
  root_dir = r''
  # 将要导入的数据表
  table = 'Head_pic'
直接修改配置文件config.json
```



#### 3、用python解释器执行insert_blob.py



#### change log:

2022-06-08:

1、添加配置文件

2、遍历文件夹支持通配符（glob库）

2022-06-10：

1、有几个文件名比较长的文件一直没有插入成功，也没有提示错误。查看数据库后，发现是把文件名截短后插入了。下次查询的时候可不是查不到了吗，文件名那一列 ，字段还要加长。或者是插入前检查一下文件名是不是超出了50个字符。

2022-06-27：

根据文件列表的长度来判断是否要sleep
2022-07-25:
处理某个文件没有读取权限时的异常处理。
返回文件二进制和md5值合并到一个函数中，这样就只读取一次文件就可以了。