<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/30
 * Time: 下午9:50
 */
// puller 单次 curl 最多拉取数据条数
define('MAXPULLCOUNT',1000);

// logmonitor 在接收到kill 信号后dump日志单次最高数量
define('DUMPCOUNT',100);

// consumer  batch 操作日志最大值
define('BATCHCOUNT',100);

// consumer 在接收到kill 信号的时候最长等待时间(单位：秒)
define('CONSUMERWAIT',5);

// children process prefix
define('PROPREFIX','pid_');

// check shard process prefix
define('SHARDPREFIX','shard_');

// define Fend_log prefix
define('LOG_PREFIX','Pull_Ali_Log_');

if(file_exists('/proc/sys/kernel/pid_max')) {
    $pidMax = file_get_contents('/proc/sys/kernel/pid_max');
    define('PIDMAX',intval($pidMax));
}
else {
    define('PIDMAX',-1);
}

class Macro{}