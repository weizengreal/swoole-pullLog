<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:41
 */
class AnalyticsMirror implements ConsumerInterface
{

    public function batchConsume($logs,$params)
    {
        foreach ($logs as $log) {
            $this->handleOne($log);
        }
    }

    private function handleOne($logInfo) {
        $mirrorType = $this->getEventType($logInfo);
        switch ($mirrorType) {
            case 'face_mirror': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['ali_time'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'classid' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'score' => $logInfo['score'],
                    'cur_time' => $logInfo['cur_timestamp'],
                    'listen_rate' => $logInfo['listen_rate'],
                );
                $faceMirror = new Model_Monitor_FaceMirror();
                $errorInfo = [];
                $ret = $faceMirror->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update face mirror fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_mirror", __FILE__, __LINE__,$record);
                }
                break;
            }
            default : {
                // 其他的信息记录本地数据库却不录入数据库，直接out
                $logInfo['__reason__'] = 'This log does not trigger logging rules';
                Fend_Log::alarm( LOG_PREFIX."Consumer_mirror", __FILE__, __LINE__,$logInfo);
                break;
            }
        }
    }

    private function getEventType($logInfo) {
        $filesArr = [
            'face_mirror'=>[
                'LESSON_FACE_SUM',
            ],
        ];
        foreach ($filesArr as $index => $events) {
            if(in_array($logInfo['event_name'],$events)) {
                return $index;
            }
        }
        return '';
    }
}