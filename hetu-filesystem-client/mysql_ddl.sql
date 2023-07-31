drop table `olk_fs_catalog` if exists;

CREATE TABLE `olk_fs_catalog`
(
    `catalog_name` varchar(256) NOT NULL COMMENT '目录名称',
    `metadata`     text COMMENT '元数据',
    `properties`   text COMMENT '配置信息',
    `create_time`  datetime DEFAULT NULL COMMENT '配置信息',
    PRIMARY KEY (`catalog_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;