<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2018/4/23
 * Time: 下午2:38
 */

// 初始化系统变量


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

//定制目录符合
define('FD_DS', DIRECTORY_SEPARATOR);

// 设置根路径
define("ROOT_PATH",__DIR__.FD_DS);

if(is_file('/proc/sys/kernel/pid_max')) {
    $pidMax = file_get_contents('/proc/sys/kernel/pid_max');
    define('PIDMAX',intval($pidMax));
}
else {
    define('PIDMAX',-1);
}


function serverHandleFatal()
{
    $error = error_get_last();
    if (isset($error['type'])) {
        switch ($error['type']) {
            case E_ERROR :
            case E_PARSE :
            case E_CORE_ERROR :
            case E_COMPILE_ERROR :
                $message = $error['message'];
                $file = $error['file'];
                $line = $error['line'];
                $log = "$message ($file:$line)\nStack trace:\n";
                $trace = debug_backtrace();
                foreach ($trace as $i => $t) {
                    if (!isset($t['file'])) {
                        $t['file'] = 'unknown';
                    }
                    if (!isset($t['line'])) {
                        $t['line'] = 0;
                    }
                    if (!isset($t['function'])) {
                        $t['function'] = 'unknown';
                    }
                    $log .= "#$i {$t['file']}({$t['line']}): ";
                    if (isset($t['object']) and is_object($t['object'])) {
                        $log .= get_class($t['object']) . '->';
                    }
                    $log .= "{$t['function']}()\n";
                }
                \EagleEye\Classes\Log::exception(LOG_PREFIX."End", "pid:" . getmypid() . " " . $log,
                    isset($t['file'])?$t['file']:__FILE__,isset($t['line']) && is_int($t['line'])?$t['line']:__LINE__);
                break;
            default:
                break;
        }
    }
}

register_shutdown_function('serverHandleFatal');