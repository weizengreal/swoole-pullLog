<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:41
 */
namespace Home\Consume;
use \EagleEye\Classes\EagleEye;

class EagleEyeJs implements \Home\Base\ConsumerInterface
{
    public function batchConsume($logs,$params)
    {
        $logContent = '';
        foreach ($logs as $log) {
            $log['x_timestamp'] = $log['ali_time'];
            $log = EagleEye::formatLog($log);
            $logContent = json_encode($log,JSON_UNESCAPED_UNICODE) . PHP_EOL;
        }
        file_put_contents(ROOT_PATH . "records/eagleeye/" . date("Y-m-d") . "_" . php_sapi_name() . ".log", $logContent, FILE_APPEND);
    }
}