<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:40
 */
class AnalyticsIos implements ConsumerInterface
{

    public function batchConsume($logs,$params)
    {
        foreach ($logs as $log) {
            $this->handleOne($log);
        }
    }

    private function handleOne($logInfo) {
        $inactType = $this->getEventType($logInfo);
        $this->dataPretreat($logInfo);
        switch ($inactType) {
            case 'sh_ipad_action': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'resume_from_background' => $logInfo['resume_from_background'],
                    'event_duration' => $logInfo['event_duration'],
                );
                $appActionModel = new Model_Monitor_IpadAction();
                $errorInfo = [];
                $ret = $appActionModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app action fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_action", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_page': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'page_name' => $logInfo['page_name'],
                    'page_description' => $logInfo['page_description'],
                    'page_referrer' => $logInfo['page_referrer'],
                    'page_start_time' => $logInfo['page_start_time'],
                    'page_start_time_msec' => $logInfo['page_start_time_msec'],
                    'event_duration' => $logInfo['event_duration'],
                );
                $appPageModel = new Model_Monitor_IpadPage();
                $errorInfo = [];
                $ret = $appPageModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app page action fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_page_action", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_element': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'element_type' => $logInfo['element_type'],
                    'element_title' => $logInfo['element_title'],
                    'page_name' => $logInfo['page_name'],
                    'page_description' => $logInfo['page_description'],
                    'call_back_function' => $logInfo['call_back_method'],
                    'element_id' => $logInfo['element_id'],
                    'list_num' => $logInfo['list_num'],
                );
                $appElementModel = new Model_Monitor_IpadElement();
                $errorInfo = [];
                $ret = $appElementModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app element fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_element", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_exception': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'exception_name' => $logInfo['exception_name'],
                    'exception_reason' => $logInfo['exception_reason'],
                    'exception_detail' => $logInfo['exception_detail'],
                );
                $appExceptionModel = new Model_Monitor_IpadException();
                $errorInfo = [];
                $ret = $appExceptionModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app exception fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_exception", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_network': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'event_duration' => $logInfo['event_duration'],
                    'is_success' => $logInfo['is_success'],
                    'request_data' => $logInfo['request_data'],
                    'response_data' => $logInfo['response_data'],
                );
                $appNetworkModel = new Model_Monitor_IpadNetwork();
                $errorInfo = [];
                $ret = $appNetworkModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app network fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_network", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_tutor_watch': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'live_number_id' => $logInfo['live_number_id'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'interaction_id' => $logInfo['interaction_id'],
                    'referrer_type' => $logInfo['referrer_type'],
                    'stu_answer' => $logInfo['stuAnswer'],
                );
                $appTutorModel = new Model_Monitor_IpadTutor();
                $errorInfo = [];
                $ret = $appTutorModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app tutor fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_tutor", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_ipad_report_view': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'app_name' => $logInfo['app_name'],
                    'app_version' => $logInfo['app_version'],
                    'os' => $logInfo['os'],
                    'os_version' => $logInfo['os_version'],
                    'device_id' => $logInfo['device_id'],
                    'brand' => $logInfo['brand'],
                    'model' => $logInfo['model'],
                    'screen' => $logInfo['screen'],
                    'network' => $logInfo['network'],
                    'ip' => $logInfo['ip'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],

                    'live_number_id' => $logInfo['live_number_id'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'lesson_date_time' => $logInfo['lesson_date_time'],
                    'class_name' => $logInfo['class_name'],
                    'page_start_time' => $logInfo['page_start_time'],
                    'page_start_time_msec' => $logInfo['page_start_time_msec'],
                    'page_end_time' => $logInfo['page_end_time'],
                    'page_end_time_msec' => $logInfo['page_end_time_msec'],
                    'event_duration' => $logInfo['event_duration'],
                );
                $appReportModel = new Model_Monitor_IpadReport();
                $errorInfo = [];
                $ret = $appReportModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh app tutor fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_report_view", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'sh_homework_correct_time': {
                $record=array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'correct_start_time' => $logInfo['correct_start_time'],
                    'correct_end_time' => $logInfo['correct_end_time'],
                    'correct_duration' => $logInfo['correct_duration'],
                    'upload_start_time' => $logInfo['upload_start_time'],
                    'upload_end_time' => $logInfo['upload_end_time'],
                    'upload_duration' => $logInfo['upload_duration'],
                    'upload_result' => $logInfo['upload_result'],
                    'referrer_client' => $logInfo['referrer_client'],
                    'tch_id' => $logInfo['tch_id'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'lesson_id' => $logInfo['lesson_id'],
                    'stu_id' => $logInfo['stu_id'],
                    'ips_course_level_name' => $logInfo['ips_course_level_name'],
                    'is_ips_inclass' => $logInfo['is_ips_inclass'],
                    'ips_status_id' => $logInfo['ips_status_id'],
                    'ips_temp_id' => $logInfo['ips_temp_id'],
                    'ips_interaction_id' => $logInfo['ips_interaction_id'],
                    'ips_pushed_qst_id' => $logInfo['ips_pushed_qst_id'],
                    'ips_paper_id' => $logInfo['ips_paper_id'],
                    'xes_is_cycle' => $logInfo['xes_is_cycle'],
                    'xes_task_type' => $logInfo['xes_task_type'],
                    'xes_publish_work_id' => $logInfo['xes_publish_work_id'],
                    'xes_event_id' => $logInfo['xes_event_id'],
                    'xes_paper_id' => $logInfo['xes_paper_id'],
                    'xes_paper_name' => $logInfo['xes_paper_name'],
                    'xes_ques_nums' => $logInfo['xes_ques_nums'],
                    'ips_ques_num' => $logInfo['ips_ques_num'],
                    'ques_id' => $logInfo['ques_id'],
//                    'paper' => $logInfo['paper'],
                    'answer_detail_id' => $logInfo['answer_detail_id'],
                    'correct_task_ques_sum' => $logInfo['correct_task_ques_sum'],
                    'picture_ques_sum' => $logInfo['picture_ques_sum'],
                    'vedio_ques_sum' => $logInfo['vedio_ques_sum'],
                    'correct_result' => $logInfo['correct_result'],
                    'evaluation' => $logInfo['evaluation'],
                    'correct_type' => $logInfo['correct_type'],
                    'video_time' => $logInfo['video_time'],
                    'video_volume' => $logInfo['video_volume'],
                );
                $correctTime = new Model_Monitor_CorrectTime();
                $errorInfo = [];
                $ret = $correctTime->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update sh homework correct time fail:'.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_correct", __FILE__, __LINE__,$record);
                }
                break;
            }
            default : {
                // 其他的信息记录本地数据库却不录入数据库，直接out
                $logInfo['__reason__'] = 'This log does not trigger logging rules';
                Fend_Log::alarm(LOG_PREFIX."Consumer_sh_app", __FILE__, __LINE__,$logInfo);
                break;
            }
        }
    }

    /*
     * 数据预处理函数，主要做全局兼容部分，比如说时间的毫秒处理、学生总学分的过滤处理
     * 点击互动数据:1，点击更多列表:2，点击左边按钮:3，点击右边按钮:4
     * */
    private function dataPretreat(& $logInfo) {
        if(!empty($logInfo['event_time'])) {
            if(strlen($logInfo['event_time']) > 10) {
                // 保留后面的几位到毫秒数中
                $timeArr = str_split($logInfo['event_time'],10);
                $event_time = $timeArr[0];
                $msec = $timeArr[1];
            }
            else {
                $event_time = $logInfo['event_time'];
                $msec = 0;
            }
            $logInfo['event_time'] = $event_time;
            $logInfo['event_msec'] = $msec;
        }

        if(!empty($logInfo['page_start_time'])) {
            if(strlen($logInfo['page_start_time']) > 10) {
                // 保留后面的几位到毫秒数中
                $timeArr = str_split($logInfo['page_start_time'],10);
                $event_time = $timeArr[0];
                $msec = $timeArr[1];
            }
            else {
                $event_time = $logInfo['page_start_time'];
                $msec = 0;
            }
            $logInfo['page_start_time'] = $event_time;
            $logInfo['page_start_time_msec'] = $msec;
        }

        if(!empty($logInfo['page_end_time'])) {
            if(strlen($logInfo['page_end_time']) > 10) {
                // 保留后面的几位到毫秒数中
                $timeArr = str_split($logInfo['page_end_time'],10);
                $event_time = $timeArr[0];
                $msec = $timeArr[1];
            }
            else {
                $event_time = $logInfo['page_end_time'];
                $msec = 0;
            }
            $logInfo['page_end_time'] = $event_time;
            $logInfo['page_end_time_msec'] = $msec;
        }

        if(!empty($logInfo['referrer_type'])) {
            if($logInfo['referrer_type'] == '点击互动数据') {
                $logInfo['referrer_type'] = 1;
            }
            else if($logInfo['referrer_type'] == '点击更多列表') {
                $logInfo['referrer_type'] = 2;
            }
            else if($logInfo['referrer_type'] == '点击左边按钮') {
                $logInfo['referrer_type'] = 3;
            }
            else if($logInfo['referrer_type'] == '点击右边按钮') {
                $logInfo['referrer_type'] = 4;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of referrer_type';
                Fend_Log::info(LOG_PREFIX."Consumer_ipad", __FILE__, __LINE__,$logInfo);
                $logInfo['referrer_type'] = 5;
            }
        }
        if(!empty($logInfo['resume_from_background'])) {
            if($logInfo['resume_from_background'] == 'true') {
                $logInfo['resume_from_background'] = 1;
            }
            else if($logInfo['resume_from_background'] == 'false') {
                $logInfo['resume_from_background'] = 2;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of resume_from_background';
                Fend_Log::info(LOG_PREFIX."Consumer_ipad", __FILE__, __LINE__,$logInfo);
                $logInfo['resume_from_background'] = 3;
            }
        }
        if(!empty($logInfo['is_success'])) {
            if($logInfo['is_success'] == 'true') {
                $logInfo['is_success'] = 1;
            }
            else if($logInfo['is_success'] == 'false') {
                $logInfo['is_success'] = 2;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of is_success';
                Fend_Log::info(LOG_PREFIX."Consumer_ipad", __FILE__, __LINE__,$logInfo);
                $logInfo['is_success'] = 3;
            }
        }

        if(!empty($logInfo['is_ips_inclass'])) {
            if($logInfo['is_ips_inclass'] = 'Y') {
                $logInfo['is_ips_inclass'] = 1;
            }
            else if($logInfo['is_ips_inclass'] = 'N') {
                $logInfo['is_ips_inclass'] = 2;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of is_ips_inclass';
                Fend_Log::info(LOG_PREFIX."Consumer_ipad", __FILE__, __LINE__,$logInfo);
                $logInfo['is_ips_inclass'] = 3;
            }
        }

        if(!empty($logInfo['xes_task_type'])) {
            if($logInfo['xes_task_type'] = 'Y') {
                $logInfo['xes_task_type'] = 1;
            }
            else if($logInfo['xes_task_type'] = 'N') {
                $logInfo['xes_task_type'] = 2;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of xes_task_type';
                Fend_Log::info(LOG_PREFIX."Consumer_ipad", __FILE__, __LINE__,$logInfo);
                $logInfo['xes_task_type'] = 3;
            }
        }

        if(!empty($logInfo['correct_result'])) {
            if($logInfo['correct_result'] = 'Y') {
                $logInfo['correct_result'] = 1;
            }
            else if($logInfo['correct_result'] = 'N') {
                $logInfo['correct_result'] = 2;
            }
            else {
                // 不符合预期的情况，记录日志，找开发
                $logInfo['__reason__'] = 'This log is wrong because of correct_result';
                Fend_Log::info(LOG_PREFIX."Consumer_sh_app", __FILE__, __LINE__,$logInfo);
                $logInfo['correct_result'] = 3;
            }
        }

    }

    private function getEventType($logInfo) {
        $filesArr = [
            'sh_ipad_action'=>[
                'dts_statistics_appStart',
                'dts_statistics_appEnd',
                'dts_statistics_login',
                'dts_statistics_logout',
            ],
            'sh_ipad_page'=>[
                'dts_statistics_pageAppear',
                'dts_statistics_pageDisappear',
            ],
            'sh_ipad_element'=>[
                'dts_statistics_clickEvent',
            ],
            'sh_ipad_exception'=>[
                'dts_statistics_exception',
            ],
            'sh_ipad_network'=>[
                'dts_statistics_networkRequest',
            ],
            'sh_ipad_tutor_watch'=>[
                'dts_statistics_choice_data',
                'dts_statistics_choice_data_list',
            ],
            'sh_ipad_report_view'=>[
                'dts_statistics_report_view',
            ],
            'sh_homework_correct_time'=>[
                'dts_statistics_tutor_correct_paper_start',
                'dts_statistics_tutor_correct_paper_end',
                'dts_statistics_tutor_correct_ques_end',
                'dts_statistics_tutor_correct_upload_vedio',
            ]
        ];
        foreach ($filesArr as $index => $events) {
            if(in_array($logInfo['event_name'],$events)) {
                return $index;
            }
        }
        return '';
    }

}