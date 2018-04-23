<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:42
 */
class LiveMonitor implements ConsumerInterface
{

    public function batchConsume($logs,$params)
    {
        // 目前文件名称就是 pid ，因为数据量很少
        $fileName = $GLOBALS['_cfg']['sys_rootdir'].'logs/aliyunpulllog/elastic/ppflow_'.$params['myPid'].'.log';
        $bulkContent = '';
        foreach ($logs as $logInfo) {
            $logInfo = $this->dataPretreat($logInfo);
            if(!empty($logInfo['Action'])) {
                $logInfo['document_type'] = strtolower($logInfo['Action']);
            }
            else {
                $logInfo['document_type'] = 'other';
            }
            if(!empty($logInfo['ali_time'])) {
                $logInfo['document_index'] = 'live_cycle_'.date('Ymd',$logInfo['ali_time']);
            }
            else {
                $logInfo['document_index'] = 'live_cycle_'.date('Ymd');
            }
            $bulkContent .= json_encode($logInfo,JSON_UNESCAPED_UNICODE).PHP_EOL;
        }
        // 落地到 logstash
        file_put_contents($fileName,$bulkContent,FILE_APPEND);
    }

    private function dataPretreat($logInfo) {
        if(isset($logInfo['room_name'])) {
            $logInfo['class_room'] = $logInfo['room_name'];
            unset($logInfo['room_name']);
        }
        $suppleArr = [
            'class_id',
            'class_name',
            'class_room',
            'pc_guid',
            'lesson_id',
        ];
        foreach ($suppleArr as $it) {
            if(!isset($logInfo[$it])) {
                $logInfo[$it] = 0;
            }
        }
        return $logInfo;
    }
}