<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:39
 */
class AnalyticsJava implements ConsumerInterface
{

    public function batchConsume($logs,$params)
    {
        foreach ($logs as $log) {
            $this->handleOne($log);
        }
    }

    private function handleOne($logInfo)
    {
        $inactType = $this->getEventType($logInfo);
        $this->dataPretreat($logInfo);
        switch ($inactType) {
            case 'interaction_middle_major': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'interaction_id' => $logInfo['interactionId'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'curtain_status' => $logInfo['curtain_status'],
                    'live_type' => $logInfo['live_type'],
                    'lesson_stu_sum' => $logInfo['lesson_stu_sum'],
                    'total_point' => $logInfo['totalPoint'],
                    'per_nums' => $logInfo['perNums'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],
                    'area_code' => $logInfo['area_code'],
                    'terminal_type' => $logInfo['terminalType'],
                    'create_time' => $logInfo['ali_time'],
                    'set_duration' => $logInfo['set_duration'],
                    'set_choice' => $logInfo['set_choice'],
                    'set_point' => $logInfo['set_point'],
                    'stu_num_sit' => $logInfo['stu_num_sit'],
                    'red_mult' => $logInfo['red_mult'],
                    'is_callback_success' => $logInfo['is_callback_success'],
                    'item_count' => $logInfo['item_count'],
                    'item_id_name' => $logInfo['item_id_name'],
                    'stu_answer_seconds' => $logInfo['stuAnswerSeconds'],
                    'tutor_answer_seconds' => $logInfo['tutorAnswerSeconds'],
                    'interaction_id_red' => $logInfo['interaction_id_red'],
                    'oper_mode' => $logInfo['oper_mode'],
                    'red_stu_remain' => $logInfo['red_stu_remain'],
                    'red_point_remain' => $logInfo['red_point_remain'],
                );
                $middleMajorModel = new Model_Monitor_MiddleMajor();
                $errorInfo = [];
                $ret = $middleMajorModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update interaction middle major fail | '.json_encode($errorInfo);
                    Fend_Log::alarm(LOG_PREFIX."Consumer_Major", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'interaction_middle_tutor': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'interaction_id' => $logInfo['interactionId'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'lesson_stu_sum' => $logInfo['lesson_stu_sum'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],
                    'right_stu_nums' => $logInfo['rightStuNums'],
                    'false_stu_nums' => $logInfo['falseStuNums'],
                    'create_time' => $logInfo['ali_time'],
                    'red_rate' => $logInfo['red_mult'],
                    'scale_basic' => $logInfo['scale_basic'],
                    'scale_extra' => $logInfo['scale_extra'],
                );
                $middleTutorModel = new Model_Monitor_MiddleTutor();
                $errorInfo = [];
                $ret = $middleTutorModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update interaction middle tutor fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Tutor", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'interaction_middle_stu': {
                if( $logInfo['event_name'] == 'STU_INTERACTION_JUDGMENT' && !isset($logInfo['event_time'])) {
                    // 这部分是脏数据，开发说事件为 STU_INTERACTION_JUDGMENT 时若不存在事件时间说明是脏数据，清掉
                    $logInfo['__reason__'] = 'STU_INTERACTION_JUDGMENT Lose param event_time';
                    Fend_Log::info(LOG_PREFIX."Consumer_Stu", __FILE__, __LINE__,$logInfo);
                    break ;
                }
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'class_id' => $logInfo['class_id'],
                    'interaction_id' => $logInfo['interactionId'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'tut_id' => $logInfo['tut_id'],
                    'tut_name' => $logInfo['tut_name'],
                    'live_type' => $logInfo['live_type'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'stu_id' => $logInfo['stu_id'],
                    'stu_name' => $logInfo['stu_name'],
                    'terminal_type' => $logInfo['terminalType'],
                    'stu_answer' => $logInfo['stuAnswer'],
                    'action_type' => $logInfo['action_type'],
                    'stu_rank' => $logInfo['stuRank'],
                    'point' => $logInfo['point'],
                    'create_time' => $logInfo['ali_time'],
                    'is_in' => $logInfo['is_in'],
                    'is_right' => $logInfo['is_right'],
                    'attention_score' => $logInfo['careValue'],
                    'photo_url' => $logInfo['photo_url'],
                    'stu_times' => $logInfo['stu_times'],
                    'stu_score' => $logInfo['stu_score'],
                    'attend_type' => $logInfo['attend_type'],
                );
                $middleStuModel = new Model_Monitor_MiddleStu();
                // 优先插入 interaction_middle_stu_all 表的全量数据
                $errorInfo = [];
                $ret = $middleStuModel->addToAll($record,$errorInfo);
                if (!$ret ) {
                    $record['__reason__'] = 'add or update interaction middle stu all fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Stu_All", __FILE__, __LINE__,$record);
                }

                // 根据逻辑检测是否需要插入数据到 interaction_middle_stu 表
                if($logInfo['event_name'] == 'STU_INTERACTION_CHOICE' || $logInfo['event_name'] == 'STU_INTERACTION_JUDGMENT') {
                    if($logInfo['is_in'] != 1 && $logInfo['is_in'] != 2) {
                        $logInfo['__reason__'] = 'old table not need absent';
                        Fend_Log::info(LOG_PREFIX."Consumer_Stu", __FILE__, __LINE__,$logInfo);
                        break;
                    }
                }
                else if($logInfo['event_name'] == 'STU_RED_POINT') {
                    if($logInfo['is_in'] != 1 || $logInfo['point'] <= 0) {
                        $logInfo['__reason__'] = 'old table not need not_in or absent or point<0';
                        Fend_Log::info(LOG_PREFIX."Consumer_Stu", __FILE__, __LINE__,$logInfo);
                        break;
                    }
                }
                else if($logInfo['event_name'] == 'STU_PRETEST' || $logInfo['event_name'] == 'STU_AFTERTEST' || $logInfo['event_name'] == 'STU_EVALUATE_TCH' || $logInfo['event_name'] == 'STU_EVALUATE_RED_POINT') {
                    if($logInfo['is_in'] != 1) {
                        $logInfo['__reason__'] = 'old table not need not_in or absent';
                        Fend_Log::info(LOG_PREFIX."Consumer_Stu", __FILE__, __LINE__,$logInfo);
                        break;
                    }
                }
                $errorInfo = [];
                $ret = $middleStuModel->add($record,$errorInfo);
                if (!$ret ) {
                    $record['__reason__'] = 'add or update interaction middle stu fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Stu", __FILE__, __LINE__,$record);
                }

                break;
            }
            case 'tug_game_start': {
                if($logInfo['event_name'] == 'PLAY_TIME_OPEN') {
                    $action = 1;
                }
                else if($logInfo['event_name'] == 'PLAY_TIME_CLOSE') {
                    $action = 2;
                }
                else {
                    // 其他的信息不记录，直接out
                    $logInfo['__reason__'] = 'This log does not trigger logging rules';
                    Fend_Log::info(LOG_PREFIX."Consumer_gamestart", __FILE__, __LINE__,$logInfo);
                    break;
                }
                $record = array(
                    'game_id' => 1, // 默认为1
                    'teacher_id' => $logInfo['tch_id'],
                    'teacher_name' => $logInfo['tch_name'],
                    'teacher_role' => $logInfo['tch_role'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'lesson_stu_sum' => $logInfo['lesson_stu_sum'],
                    'interaction_id' => $logInfo['interactionId'],
                    'action_type' => $action,
                    'create_time' => $logInfo['event_time'],
                    'time_msec' => $logInfo['event_msec'],
                );
                $gameStartModel = new Model_Monitor_GameStart();
                $errorInfo = [];
                $ret = $gameStartModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update interaction game start fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Gamestart", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'tug_game_result': {
                $record = array(
                    'live_number_id' => $logInfo['live_number_id'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_id' => '',
                    'lesson_num' => $logInfo['lesson_num'],
                    'teacher_id' => $logInfo['tch_id'],
                    'teacher_name' => $logInfo['tch_name'],
                    'teacher_role' => $logInfo['tch_role'],
                    'city_code' => $logInfo['area_code'],
                    'classroom_name' => $logInfo['classroom_name'],
                    'stu_count' => $logInfo['lesson_stu_sum'],
                    'game_id' => $logInfo['game_id'],
                    'round_num' => $logInfo['game_round'],
                    'is_win' => $logInfo['is_win'],
                    'interaction_id' => $logInfo['interactionId'],
                    'create_date' => $logInfo['event_time'],
                );
                $gameResultModel = new Model_Monitor_GameResult();
                $errorInfo = [];
                $ret = $gameResultModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update interaction game result fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Gameresult", __FILE__, __LINE__,$record);
                }
                break;
            }
            case 'interaction_after_point': {
                $record = array(
                    'event_name' => $logInfo['event_name'],
                    'event_time' => $logInfo['event_time'],
                    'event_msec' => $logInfo['event_msec'],
                    'point_source' => $logInfo['point_source'],
                    'point_visible' => $logInfo['point_visible'],
                    'point_notvisible' => $logInfo['point_notvisible'],
                    'ponit_sum_visible' => $logInfo['ponit_sum_visible'],
                    'ponit_sum_notvisible' => $logInfo['ponit_sum_notvisible'],
                    'tch_id' => $logInfo['tch_id'],
                    'tch_role' => $logInfo['tch_role'],
                    'tch_name' => $logInfo['tch_name'],
                    'live_number_id' => $logInfo['live_number_id'],
                    'class_id' => $logInfo['class_id'],
                    'lesson_num' => $logInfo['lesson_num'],
                    'stu_count' => $logInfo['stu_count'],
                    'stu_id' => $logInfo['stu_id'],
                    'stu_name' => $logInfo['stu_name'],
                );
                $afterPointModel = new Model_Monitor_AfterPoint();
                $errorInfo = [];
                $ret = $afterPointModel->add($record,$errorInfo);
                if (!$ret) {
                    $record['__reason__'] = 'add or update interaction after point fail | '.json_encode($errorInfo);
                    Fend_Log::info(LOG_PREFIX."Consumer_Point", __FILE__, __LINE__,$record);
                }
                break;
            }
            default : {
                // 其他的信息记录本地数据库却不录入数据库，直接out
                $record['__reason__'] = 'This log does not trigger logging rules';
                Fend_Log::alarm(LOG_PREFIX."Consumer_Interaction", __FILE__, __LINE__,$record);
                break;
            }
        }
    }

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
        if(!empty($logInfo['is_win'])) {
            if($logInfo['is_win'] == 'victory') {
                $logInfo['is_win'] = 1;
            }
            else if ($logInfo['is_win'] == 'defeat') {
                $logInfo['is_win'] = 2;
            }
            else {
                $logInfo['__reason__'] = 'This log is wrong because of is_win';
                Fend_Log::info(LOG_PREFIX."Java_Data_Pretreat", __FILE__, __LINE__,$logInfo);
                $logInfo['is_win'] = intval($logInfo['is_win']);
            }
        }

        if(!empty($logInfo['is_in'])) {
            $logInfo['is_in'] = trim($logInfo['is_in']);
            if($logInfo['is_in'] == 'true') {
                $logInfo['is_in'] = 1;
            }
            else if ($logInfo['is_in'] == 'false') {
                $logInfo['is_in'] = 2;
            }
            // 1215 改版
            else if ($logInfo['is_in'] == 'in') {
                $logInfo['is_in'] = 1;
            }
            else if ($logInfo['is_in'] == 'not_in') {
                $logInfo['is_in'] = 2;
            }
            else if ($logInfo['is_in'] == 'absent') {
                $logInfo['is_in'] = 3;
            }
            else {
                $logInfo['__reason__'] = 'This log is wrong because of is_in';
                Fend_Log::info(LOG_PREFIX."Java_Data_Pretreat", __FILE__, __LINE__,$logInfo);
                $logInfo['is_in'] = -1;
            }
        }


        if(!empty($logInfo['is_right'])) {
            if($logInfo['is_right'] == 'true') {
                $logInfo['is_right'] = 1;
            }
            else if ($logInfo['is_right'] == 'false') {
                $logInfo['is_right'] = 2;
            }
            else {
                $logInfo['__reason__'] = 'This log is wrong because of is_right';
                Fend_Log::info(LOG_PREFIX."Java_Data_Pretreat", __FILE__, __LINE__,$logInfo);
                $logInfo['is_right'] = 3;
            }
        }

        if(!empty($logInfo['is_callback_success'])) {
            if($logInfo['is_callback_success'] == 'true') {
                $logInfo['is_callback_success'] = 1;
            }
            else if ($logInfo['is_callback_success'] == 'false') {
                $logInfo['is_callback_success'] = 2;
            }
            else {
                $logInfo['__reason__'] = 'This log is wrong because of is_callback_success';
                Fend_Log::info(LOG_PREFIX."Java_Data_Pretreat", __FILE__, __LINE__,$logInfo);
                $logInfo['is_callback_success'] = 3;
            }
        }

        if(isset($logInfo['_student_score'])) {
            $logInfo['_student_score']=str_replace('总学分:','',$logInfo['_student_score']);
        }
    }

    private function getEventType($logInfo) {
        $filesArr = [
            'interaction_middle_major'=>[
                'IPS_EXAM_FRONT_OPEN',
                'IPS_EXAM_FRONT_CLOSE',
                'IPS_EXAM_AFTER_OPEN',
                'IPS_EXAM_AFTER_CLOSE',
                'EXAMPLE_OPEN',
                'EXAMPLE_CLOSE',
                'BREAK_TIME_OPEN',
                'BREAK_TIME_CLOSE',
                'REDPOINT_LUCK_OPEN',
                'REDPOINT_AVG_OPEN',
                'REDPOINT_CLOSE',
                'EVALUATION_OPEN',
                'EVALUATION_CLOSE',
                'EVALUATION_REDPOINT_OPEN',
                'EVALUATION_REDPOINT_CLOSE',
                'INTERACTION_CHOICE_OPEN',
                'INTERACTION_CHOICE_CLOSE',
                'INTERACTION_NON_OPEN',
                'INTERACTION_NON_CLOSE',
                'EXAMPLE_SUBJECTIVE_OPEN',
                'EXAMPLE_SUBJECTIVE_CLOSE',

                // 1020
                'INTERACTION_ATTENTION_OPEN',
                'INTERACTION_ATTENTION_CLOSE',

                // 1120
                'INTERACTION_UNDERSTAND_OPEN',
                'INTERACTION_UNDERSTAND_CLOSE',

                // 1215
                'INTERACTION_RUBRIC_OPNE',
                'INTERACTION_RUBRIC_CLOSE',

                // 0120
                'INTERACTION_CALL_NAME_OPEN',
                'INTERACTION_CALL_NAME_CLOSE',

                // 0314
                'EVALUATION_PARENT_OPEN',
                'EVALUATION_PARENT_CLOSE',
                'LCT_CURTAIN_OPEN',
                'LCT_CURTAIN_CLOSE',
            ],
            'interaction_middle_tutor'=>[
                'SEND_POINT_MANY',
                'SUBJECTIVE_RECORD',
                'INTERACTION_ATTENTION_CLASS_CLOSE',
                'CLASS_RED_MULT',

                // 0314
                'CLASS_PANEL_SCALE',
            ],
            'interaction_middle_stu'=>[
                'STU_PRETEST',
                'STU_AFTERTEST',
                'STU_HOMEWORK_STAR',
                'STU_RED_POINT',
                'STU_EVALUATE_TCH',
                'STU_EVALUATE_RED_POINT',
                'STU_INTERACTION_CHOICE',
                'STU_INTERACTION_JUDGMENT',
                'STU_SEND_POINT_MANY',
                'STU_RIGHT_RANK',

                'STU_ATTENTION_START',
                'STU_MVP',

                // 1215
                'STU_PRACTICE_STAR',

                // 0314
                'STU_CHECK_ATTEND',
                'STU_PART_RANK',
            ],
            'tug_game_start'=>[
                'PLAY_TIME_OPEN',
                'PLAY_TIME_CLOSE',
            ],
            'tug_game_result'=>[
                'PLAY_TIME_TUG'
            ],
            'interaction_after_point'=>[
                'TCH_POINT_ISSUE',
                'TCH_POINT_REMAIN'
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