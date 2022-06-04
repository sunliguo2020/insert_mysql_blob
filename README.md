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

