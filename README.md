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
