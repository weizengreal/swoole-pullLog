<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/9
 * Time: 下午6:10
 */

return [
    'server'      => [
        'name' => 'pull_log_dev',
        'host' => '0.0.0.0',
        'port' => '9201',

        "recordpath"     => ROOT_PATH.'records/recordDb/tableRecord.db',
        "logpath"        => ROOT_PATH.'records/log/',
        "channelpath"    => ROOT_PATH.'records/channel/',
        "tablesize"      => 1024,
        "loglevel"       => 4,
        "processtimeout" => 10,//进程超时时间，超过10秒自动kill
        "aliyun_salt"    => "weizengreal"
    ],
    // TODO:: 日志路径等名称上线需要修改
    'swoole'      => [
        'dispatch_mode'      => 3,
        'package_max_length' => 2097152,
        // 1024 * 1024 * 2,
        'buffer_output_size' => 3145728,
        //1024 * 1024 * 3,
        'pipe_buffer_size'   => 33554432,
        //1024 * 1024 * 32,
        'open_tcp_nodelay'   => 1,
        'open_cpu_affinity'  => 1,
        'worker_num'         => 1,
        'max_request'        => 200,
        'log_level'          => 2,
        //swoole 日志级别 Info
        'daemonize'          => 1,
        //生产打开为1
        'backlog'            => 100,
        'log_file'           => ROOT_PATH.'records/caches/aliyunsis_dev.log',
        //swoole 系统日志，任何代码内echo都会在这里输出
        'task_tmpdir'        => '/dev/shm/sismonitor_dev/',
        'pid_file'           => ROOT_PATH.'records/caches/pulllog_dev.pid',
    ],
    'aliyun'      => [
        'doubleteacher_js_eagleeye'          => [
            'pullClass'     => 'OnlineJs',
            'consumeClass'  => 'EagleEyeJs',
            'consumerCount' => 4,
        ],
    ],
    'defaultConf' => [
//         "endpoint" => 'cn-beijing.log.aliyuncs.com',  // 阿里云内网地址
        "endpoint"        => 'cn-beijing-intranet.log.aliyuncs.com',  // 阿里云外网地址
        // 阿里云日志拉取内网域名
        "accessKeyId"     => 'aliyunLog_accessKeyId',
        "accessKey"       => 'aliyunLog_accessKey',
        "project"         => 'aliyunLog_project',
        "token"           => "",
        'channelMaxCount' => 100,
        'channelSize'     => 1024 * 1024 * 100,
    ],
    'vaildStore'  => [
        'doubleteacher_analytics_pc_debug',
        'doubleteacher_analytics_java_debug',
        'mirror_analytics_debug',
        'doubleteacher_js_eagleeye',
    ],
];
