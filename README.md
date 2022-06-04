<<<<<<< HEAD
在mysql中保存文件。
文件的内容类型保存为blob，同时保存文件名和最后的修改时间。
为了去重，添加MD5值作为一列。也可以用来验证blob列保存是否正确。
```sql
CREATE TABLE `workers` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `file_name` char(50) NOT NULL,
  `md5sum` char(32) NOT NULL,
  `blob` mediumblob NOT NULL,
  `mod_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `md` (`md5sum`) USING HASH,
  KEY `fn` (`file_name`) USING HASH
) ENGINE=MyISAM AUTO_INCREMENT=443340 DEFAULT CHARSET=utf8mb4;

```
=======
# insert_mysql_blob
在mysql中用blob保存一些文本文件。

CREATE TABLE `sgy_idcard_pic` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `file_name` char(50) CHARACTER SET latin1 NOT NULL,
      `md5sum` char(32) CHARACTER SET latin1 NOT NULL DEFAULT '',
      `blob` mediumblob NOT NULL,
      `mod_time` datetime NOT NULL,
      PRIMARY KEY (`id`),
      KEY `md` (`md5sum`) USING HASH,
      KEY `fn` (`file_name`) USING HASH
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
>>>>>>> b538a607d07ca4402f99022686b7548b5ea62cdf
