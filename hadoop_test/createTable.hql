use ods;
drop table if exists ods.S02_ARTISAN_MOBILE_CHANNEL;
CREATE EXTERNAL TABLE ods.S02_ARTISAN_MOBILE_CHANNEL(
id bigint comment '主键ID',
artisan_id string comment '手艺人ID',
channel_id string comment '百度渠道ID',
user_id string comment '百度用户ID',
platform string comment '平台(IOS或者Android)',
create_time string comment '创建时间',
update_time string comment '更新时间',
version string comment '版本'
)
comment '手艺人设备在百度注册的信息'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_ARTISAN_MOBILE_CHANNEL';


use ods;
drop table if exists ods.S02_ARTISAN_TAG;
CREATE EXTERNAL TABLE ods.S02_ARTISAN_TAG(
id bigint comment '主键ID',
artisan_tag_id bigint comment '标签ID',
artisan_id string comment '缩写名称',
create_time string comment '建立时间',
create_user_id string comment '建立者ID',
update_time string comment '更新时间',
update_user_id string comment '更新者ID',
is_availability string comment '1：有效
            0：无效',
is_del string comment '0：未删除
            1：删除',
remark1 string comment '备注1',
remark2 string comment '备注2',
remark3 string comment '备注3'
)
comment '手艺人标签'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_ARTISAN_TAG';


use ods;
drop table if exists ods.S02_AWARD;
CREATE EXTERNAL TABLE ods.S02_AWARD(
id bigint comment '主键ID',
name string comment '奖品名称',
award_to bigint comment '0 所有用户， 1 平台新客， 2 老客',
win_count bigint comment '已抽取数量',
type string comment '类别 00： 未中奖， 10： 礼帽， 20： 普通实物， 21： 优惠券实物',
amount bigint comment '红包金额',
version bigint comment '版本或批次 向下兼容 后期若新增一个奖品版本号增一',
create_time string comment '创建时间',
update_time string comment '修改时间'
)
comment '奖品'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_AWARD';


use ods;
drop table if exists ods.S02_AWARD_COUPON;
CREATE EXTERNAL TABLE ods.S02_AWARD_COUPON(
id bigint comment '主键ID',
award_id bigint comment '奖品id',
coupon_code string comment '优惠券编码',
user_type bigint comment '用户类型(0 所有用户， 1 平台新客, 2 老客)',
user_category_type bigint comment '老客类型(1 美容用户, 2 美甲用户)',
coupon_name string comment '优惠券展示名称',
create_time string comment '创建时间'
)
comment '奖品--优惠券关联表'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_AWARD_COUPON';


use ods;
drop table if exists ods.S02_AWARD_DAILY_STOCK;
CREATE EXTERNAL TABLE ods.S02_AWARD_DAILY_STOCK(
id bigint comment '主键ID',
award_id bigint comment '奖品ID',
award_type string comment '冗余 类别 00： 未中奖， 10： 礼帽， 20： 普通实物， 21： 优惠券实物',
award_to bigint comment '0 所有用户， 1 平台新客， 2 老客',
effective_date string comment '生效日期',
stock bigint comment '该日放出库存数',
weight bigint comment '权重修改此值调整中奖概率',
version bigint comment '版本或批次 向下兼容 新增一个奖品版本号增一',
create_time string comment '创建时间',
update_time string comment '修改时间'
)
comment '奖品每日库存'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_AWARD_DAILY_STOCK';


use ods;
drop table if exists ods.S02_BAIDU_DEV;
CREATE EXTERNAL TABLE ods.S02_BAIDU_DEV(
id bigint comment '主键ID',
c_id string comment '客户端ID',
u_id string comment '用户ID',
token string comment 'token',
push_state bigint comment '推送状态',
user_id string comment '用户ID',
city_code string comment '城市编码',
create_time string comment '创建时间',
up_time string comment '更新时间',
device_info string comment '设备信息',
version string comment '版本'
)
comment '用户端设备信息，用于push'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_BAIDU_DEV';


use ods;
drop table if exists ods.S02_GOOD_COMMENT;
CREATE EXTERNAL TABLE ods.S02_GOOD_COMMENT(
id bigint comment '主键',
artisan_id string comment '手艺人ID',
product_id string comment '作品ID',
bad_star bigint comment '不满意数',
normal_star bigint comment '基本满意数',
good_star bigint comment '很满意数',
excited_star bigint comment '超出期待数',
product_good_percent bigint comment '作品好评百分比的分子，如75%就是75',
artisan_good_percent bigint comment '手艺人好评百分比的分子，如75%就是75',
product_good_percent_recent bigint comment '最近30天作品好评比',
create_time string comment '创建时间',
update_time string comment '更新时间'
)
comment '好评比，包含作品与手艺人.计算数据依据artisan_comment表'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_GOOD_COMMENT';


use ods;
drop table if exists ods.S02_GOOD_COMMENT_ARTISAN;
CREATE EXTERNAL TABLE ods.S02_GOOD_COMMENT_ARTISAN(
id bigint comment '主键',
artisan_id string comment '手艺人ID',
bad_star bigint comment '不满意数',
normal_star bigint comment '基本满意数',
good_star bigint comment '很满意数',
excited_star bigint comment '超出期待数',
artisan_good_percent bigint comment '手艺人好评百分比的分子，如75%就是75',
artisan_good_percent_recent bigint comment '手艺人近期好评百分比的分子，如75%就是75',
create_time string comment '创建时间'
)
comment '手艺人好评比，计算数据依据artisan_comment表'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/hive/db/ods/S02_GOOD_COMMENT_ARTISAN';