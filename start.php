<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2018/4/23
 * Time: 下午2:02
 */

use Home\LogMonitor;

include "init.php";

require_once "loader.php";

date_default_timezone_set('Asia/Shanghai');

// 鉴权
authenticaton();

// 解析参数
$params = parseArgvs($argv);

$config = customConf($params['f']);

$action = $params['action'];

switch ($action) {
    case 'start': {
        $LogMonitor = new LogMonitor($config);
        $LogMonitor->start();
        break;
    }
    case 'stop': {
        $pid = file_get_contents($config['swoole']['pid_file']);
        if(!swoole_process::kill($pid)) {
            die('stop faild'.PHP_EOL);
        }
        break;
    }
    case 'reload': {
        $pid = file_get_contents($config['swoole']['pid_file']);
        $cmd = "kill -s 10 $pid";
        exec($cmd, $outStr);
        break;
    }
    case 'restart': {
        $pid = file_get_contents($config['swoole']['pid_file']);
        if(!swoole_process::kill($pid)) {
            die('stop faild'.PHP_EOL);
        }
        echo 'successful send signal--sigterm,please wait seconds'.PHP_EOL;
        while (\Home\Utils\Helper::ifrun($pid)) {
            sleep(1);
        }
        // 释放端口资源
        sleep(1);
        $LogMonitor = new LogMonitor($config);
        $LogMonitor->start();
        break;
    }
    case 'kill': {
        $serverName = 'tal_'.$config['server']['name'];
        $cmd = "ps -ef | grep $serverName | grep -v grep | cut -c 9-15 | xargs kill -s 9 ";
        exec($cmd, $outStr);
        break;
    }
    default: {
        echo helpDom($config['server']['service_name']);
        break;
    }
}


/*
 * 接收一个数组
 * */
function parseArgvs($argv)
{
    $params = getopt('c:f:hp:');
    $count = count($argv);
    if (strpos($argv[$count - 2], '-') === false) {
        if ($argv[$count - 1] == '&') {
            $params['action'] = $argv[$count - 2];
        } else {
            $params['action'] = $argv[$count - 1];
        }
    }
    if (isset($params['h'])) {
        echo helpDom();
        die;
    }
    return $params;
}

/*
 * 帮助文档
 * */
function helpDom($service_name = 'server')
{
    $helpDom = $service_name . ' 1.0 ,allow params:' . PHP_EOL;
    $helpDom .= '-f:﻿用户自定义配置参数' . PHP_EOL;
//    $helpDom .= '-c:﻿系统公共配置参数' . PHP_EOL;
    $helpDom .= '-h: 查看帮助文件' . PHP_EOL;
    $helpDom .= '{start、stop、restart、reload} 服务动作' . PHP_EOL;
    return $helpDom;
}

// 鉴权函数，保证swoole的安装和root权限
function authenticaton()
{
    if (PHP_SAPI != 'cli') {
        die('please run in cli mode' . PHP_EOL);
    }
    if (!function_exists('exec')) {
        die('没有exec函数执行权限...' . PHP_EOL);
    }
    if (!extension_loaded('swoole')) {
        die('swoole extension was not found' . PHP_EOL);
    }

    if (getenv("USER") != "root") {
        die("请使用root用户执行..." . PHP_EOL);
    }
}

// 获得用户自定义配置
function customConf($confSign)
{
    $configPath = ROOT_PATH.'configs/'.$confSign;
    if (empty($configPath) || !file_exists(trim($configPath))) {
        die ("Format: php start.php -c framework_config_file_path -f server_config_file_path");
    }

    $suffix = explode('.', $configPath);
    if ($suffix[count($suffix) - 1] == 'php') {
        // php 文件直接 include
        if(!file_exists($configPath)) {
            echo 'not found ' . $configPath . PHP_EOL;
            exit;
        }
        $config = include($configPath);
    } else {
        // 其他文件默认为json
        $config = file_get_contents($configPath);
        $config = json_decode($config, true);
    }
    return $config;
}

