<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:43
 */
class LiveCycle implements ConsumerInterface
{

    public function batchConsume($logs,$params)
    {
        $fileName = $GLOBALS['_cfg']['sys_rootdir'].'logs/aliyunpulllog/elastic/ppflow_'.$params['myPid'].'_'.date('Ymd').'.log';
        $bulkContent = '';
        foreach ($logs as $logInfo) {
            $logInfo = $this->dataPretreat($logInfo);
            if(isset($logInfo['event_name'])) {
                $type = strtolower($logInfo['event_name']);
            }
            else {
                if(isset($logInfo['EncodeResolution'])) {
                    $type = 'encoderesolution';
                }
                else if(isset($logInfo['CameraParam'])) {
                    $type = 'cameraparam';
                }
                else if(isset($logInfo['Capture'])) {
                    $type = 'capture';
                }
                else if(isset($logInfo['Play'])) {
                    $type = 'play';
                }
                else if(isset($logInfo['Encode'])) {
                    $type = 'encode';
                }
                else {
                    $type = 'other';
                }
            }

            $logInfo['document_type'] = $type;
            if(!empty($logInfo['ali_time'])) {
                $logInfo['document_index'] = 'live_cycle_'.date('Ymd',$logInfo['ali_time']);
            }
            else {
//                    echo 'document_index_date is empty'.PHP_EOL;
                $logInfo['document_index'] = 'live_cycle_'.date('Ymd');
            }
            $bulkContent .= json_encode($logInfo,JSON_UNESCAPED_UNICODE) .PHP_EOL;
        }
        // 落地到 logstash
        file_put_contents($fileName,$bulkContent,FILE_APPEND);
    }

    /*
    * 数据预处理函数，主要做全局兼容部分
    * 这里将不符合预期的单位过滤掉
    * */
    private function dataPretreat($logInfo) {
        // 版本兼容，很烦
        if(isset($logInfo['audioCodingRate '])) {
            $logInfo['audioCodingRate'] = $logInfo['audioCodingRate '];
            unset($logInfo['audioCodingRate ']);
        }
        if(isset($logInfo['videoNetworkSendRate '])) {
            $logInfo['videoNetworkSendRate'] = $logInfo['videoNetworkSendRate '];
            unset($logInfo['videoNetworkSendRate ']);
        }
        $preArr = ['videoInputFrames','videoEncFrames','videoEncodeRate','videoNetworkSendRate'
            ,'audioInputFrames','audioEncFrames','audioCodingRate','audioSendCodingRate','frameRate'
            ,'maxBufferTime','videoRecvFrames','videoDecFrames','audioRecvFrames','audioDecFrames'];
        foreach ($preArr as $item) {
            if(isset($logInfo[$item])) {
                $logInfo[$item] = str_replace('fps','',$logInfo[$item]);
                $logInfo[$item] = str_replace('kbit/s','',$logInfo[$item]);
                $logInfo[$item] = str_replace('ms','',$logInfo[$item]);
                $logInfo[$item] = intval($logInfo[$item]); // 64为操作系统，足够大
            }
        }
        if(isset($logInfo['room_name'])) {
            $logInfo['class_room'] = $logInfo['room_name'];
            unset($logInfo['room_name']);
        }
        return $logInfo;
    }

}