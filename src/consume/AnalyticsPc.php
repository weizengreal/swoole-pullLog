<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/24
 * Time: 下午4:45
 */
class AnalyticsPc implements ConsumerInterface
{

    /*
     * 实现批处理方法
     * */
    public function batchConsume($logs, $params)
    {
        foreach ($logs as $log) {
            $this->handleOne($log);
        }
    }

    /*
     * 定义不支持批处理的日志拉取函数
     * */
    public function handleOne($logInfo)
    {
        if (!$this->logFilter($logInfo)) {
            return;
        }
        $this->dataPretreat($logInfo);
        if (!isset($logInfo['event_name'])) {
            /**
             * @var Fend_Redis $facRedis
             */
            $facRedis = Fend_Redis::factory();

            switch ($logInfo["answer_tool"]) {
                case "student_list":
                    /*
                     * 如果来这个数据有两个情况，
                     * 刚开课
                     * 电脑重启
                     * 老师换教室
                     *
                     * 不管什么状态
                     * 需要更新status表基础信息（根据最后更新日期对比时间）
                     * 需要统计捆绑历史内数据数
                     */
                    //binded answer tools

                    //student list format
                    $studentListString = explode(">=<", $logInfo["_list"]);
                    if (count($studentListString) != 2) {
                        $logInfo ['__reason__'] = 'bad log format';
                        Fend_Log::info(LOG_PREFIX."Consumer_Answer_Tool", __FILE__, __LINE__, $logInfo);
                        break;
                    }

                    $studentList = explode("> <", $studentListString[1]);
                    foreach ($studentList as $k => $student) {
                        $studentInfo = explode(",", trim($student, "\t\n\r>< "));
                        $studentList[$k] = array(
                            "name" => trim($studentInfo[0]),
                            "id" => trim($studentInfo[1]),
                        );
                    }

                    //answer student
                    $answerStudentModel = Model_Monitor_AnswerStudent::factory();

                    //记录student 历史日志
                    $record = array(
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'student_count' => $logInfo["_count"],
                        'student_list' => json_encode($studentList),
                        'create_time' => $logInfo["ali_time"],
                    );

                    //add student history
                    $errorInfo = array();

                    $ret = $answerStudentModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'answer student history Insert Fail1:' . json_encode($errorInfo);
                        Fend_Log::alarm(LOG_PREFIX."Consumer_student_history", __FILE__, __LINE__, $record);
                    }

                    //检测是否已经有过开始
                    //或客户端重启
                    //或换教室
                    //check exist

                    $record = array(
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'bind_detail' => "",
                        'student_list' => json_encode($studentList),
                        'student_count' => $logInfo["_count"],
                        'bind_count' => 0,
                        'status' => 0,
                        'dingding_status' => "",
                        'alarm_hour' => 0,
                        'alarm_time' => 0,
                        'solve_time' => 0,
                        'create_time' => $logInfo["ali_time"],
                    );

                    $answerStatusModel = Model_Monitor_AnswerStatus::factory();

                    //class_id|lesson_id|pc_guid
                    $where = array(
                        "class_id" => $logInfo["class_id"],
                        "lesson_id" => $logInfo["lesson_id"],
                        "pc_guid" => $logInfo["pc_guid"],
                    );
                    $result = $answerStatusModel->getList($where, array(), 0, 10);

                    //没有就是刚开课
                    //有数据就需要重置状态(pc重启或者换班)
                    $errorInfo = array();

                    if (isset($result["total"]) && $result["total"] > 0 && isset($result["list"][0]["id"])) {
                        //原有数据，进行更新
                        //那么清理掉原来的绑定
                        //$redis->del("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"]);

                        $record["id"] = $result["list"][0]["id"];
                        $ret = $answerStatusModel->add($record, $errorInfo);
                        if (!$ret) {
                            $record ['__reason__'] = 'update answer student status fail:' . json_encode($errorInfo);
                            Fend_Log::alarm(LOG_PREFIX."Consumer_status", __FILE__, __LINE__, $record);
                        }
                    } else {
                        //没有数据，直接添加
                        //if the bind already run on befor
//                        $bindInfo = $redis->hGetAll("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"]);
                        $bindInfo = $facRedis->hashGetAll("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"]);

                        $record["bind_detail"] = json_encode($bindInfo);
                        //added
                        $ret = $answerStatusModel->add($record, $errorInfo);
                        if (!$ret) {
                            $record ['__reason__'] = 'add answer student status fail:' . json_encode($errorInfo);
                            Fend_Log::alarm(LOG_PREFIX."Consumer_status", __FILE__, __LINE__, $record);
                        }
                    }

                    break;
                case "bind":
                case "unbind":

                    //binded answer tools

                    $bind = -1;
                    if ($logInfo["answer_tool"] == "bind") {
                        $bind = 0;
                    }
                    if ($logInfo["answer_tool"] == "unbind") {
                        $bind = 1;
                    }

                    $record = array(
                        'device_id' => $logInfo["_device_id"],
                        'student_id' => $logInfo["_student_id"],
                        'student_name' => $logInfo["_student_name"],
                        'bind_type' => $bind,
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'create_time' => $logInfo["ali_time"],
                    );
                    $errorInfo = array();
                    $answerBindModel = Model_Monitor_AnswerBind::factory();
                    $ret = $answerBindModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'update answer student status fail:' . json_encode($errorInfo);
                        Fend_Log::alarm(LOG_PREFIX."Consumer_bind", __FILE__, __LINE__, $record);
                    }

                    //lock   $redis->setnx("mn_lockstudent_" . $logInfo["_student_id"], 1)
                    $lock_time = 0;
                    while (!$facRedis->add("mn_lockstudent_" . $logInfo["_student_id"], 1)) {
                        usleep(200);
                        $lock_time++;
                        //4秒还没释放
                        if ($lock_time > 20) {
                            //强制去掉锁定
                            $facRedis->expire("mn_lockstudent_" . $logInfo["_student_id"], 0);
                        }
                    }

                    //expire 20 second when got the lock
                    $facRedis->expire("mn_lockstudent_" . $logInfo["_student_id"], 20);

                    //make sure the wrong order only apply the last operation
                    $bindInfo = $facRedis->hashGet("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"],
                        $logInfo["_student_id"]);

                    if ($bindInfo) {
                        $bindInfo = json_decode($bindInfo, true);
                        //exist and less
                        if (isset($bindInfo["lastupdate"]) && $bindInfo["lastupdate"] > 0 && $bindInfo["lastupdate"] <= $logInfo["ali_time"]) {

                            $facRedis->hashSet("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"],
                                $logInfo["_student_id"],
                                array("lastupdate" => $logInfo["ali_time"],
                                      "bind" => $bind,
                                      "device_id" => $logInfo["_device_id"],
                                      "student_name" => $logInfo["_student_name"])
                            );
                        }
                    } else {
                        //not exist
                        $facRedis->hashSet("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"],
                            $logInfo["_student_id"],
                            array("lastupdate" => $logInfo["ali_time"],
                                  "bind" => $bind,
                                  "device_id" => $logInfo["_device_id"],
                                  "student_name" => $logInfo["_student_name"])
                        );
                    }

                    //slowly operation
                    $bindInfo = $facRedis->hashGetAll("mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"]);

                    //var_dump("bindinfo:", "mn_" . $logInfo["lesson_id"] . "_" . $logInfo["class_id"] . "_" . $logInfo["pc_guid"], $bindInfo);

                    //update the status bind status
                    $answerStatusModel = Model_Monitor_AnswerStatus::factory();

                    //class_id|lesson_id|pc_guid
                    $where = array(
                        "class_id" => $logInfo["class_id"],
                        "lesson_id" => $logInfo["lesson_id"],
                        "pc_guid" => $logInfo["pc_guid"],
                    );

                    $result = $answerStatusModel->getList($where, array(), 0, 10);

                    //bind 可能出空数据
                    if (isset($result["total"]) && $result["total"] > 0 && isset($result["list"][0]) && is_array($result["list"][0])) {
                        $data = $result["list"][0];
                        $data["bind_detail"] = json_encode($bindInfo);

                        $bindCount = 0;
                        foreach ($bindInfo as $k => $v) {
                            $bindItem = json_decode($v, true);
                            if ($bindItem["bind"] == 0) {
                                $bindCount++;
                            }
                        }
                        $data["bind_count"] = $bindCount;
                        //test
                        if ($data["class_id"] == "") {
                            $logInfo ['__reason__'] = 'student status got bug';
                            Fend_Log::alarm(LOG_PREFIX."Consumer_bind", __FILE__, __LINE__, array($logInfo, $result, $data, $where));
                        } else {
                            $errorInfo = array();
                            $ret = $answerStatusModel->add($data, $errorInfo);
                            if (!$ret) {
                                $data ['__reason__'] = 'update status fail:' . json_encode($errorInfo);
                                Fend_Log::alarm(LOG_PREFIX."Consumer_bind_status", __FILE__, __LINE__, $data);
                            }
                        }
                    }

                    //redis end
                    //release the lock
                    $facRedis->del("mn_lockstudent_" . $logInfo["_student_id"]);

                    break;

                case "sign_up":
                    if(isset($logInfo['_game_id']) && empty($logInfo['_game_id'])) {
                        $logInfo['__reason__'] = 'bad sign_up log format:wrong game_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Signup", __FILE__, __LINE__, $logInfo);
                    }
                    if ($logInfo['_action'] == 'sign_up') {
                        // 总学分那边沟通结果：由我们后台日志方过滤
                        $record = array(
                            'game_id' => $logInfo['_game_id'],
                            'interaction_id' => $logInfo['_interaction_id'],
                            'student_id' => $logInfo['_student_id'],
                            'student_name' => $logInfo['_student_name'],
                            'student_score' => $logInfo['_student_score'],
                            'city_id' => $logInfo["city_id"],
                            'city_name' => $logInfo["city_name"],
                            'class_id' => $logInfo["class_id"],
                            'class_name' => $logInfo["class_name"],
                            'client_version' => $logInfo["client_version"],
                            'lecturer_name' => $logInfo["lecturer_name"],
                            'lesson_id' => $logInfo["lesson_id"],
                            'lesson_num' => $logInfo["lesson_num"],
                            'log_source' => $logInfo["log_source"],
                            'live_number_id' => $logInfo['live_id'],
                            'pc_guid' => $logInfo["pc_guid"],
                            'room_name' => $logInfo["room_name"],
                            'time_desc' => $logInfo["time_desc"],
                            'tutor_name' => $logInfo["tutor_name"],
                            'create_time' => $logInfo["ali_time"],
                        );
                        $studentSignModel = new Model_Monitor_StudentSign();
                        $errorInfo = [];
                        $ret = $studentSignModel->add($record, $errorInfo);
                        if (!$ret) {
                            $record['__reason__'] = 'add or update answer student signup fail:' . json_encode($errorInfo);
                            Fend_Log::info(LOG_PREFIX."Consumer_Signup", __FILE__, __LINE__, $record);
                        }
                    } else if ($logInfo['_action'] == 'start' || $logInfo['_action'] == 'stop') {
                        // 历史兼容问题，过滤掉即可
                        $logInfo['__reason__'] = 'do not need it!';
                        Fend_Log::debug(LOG_PREFIX."Consumer_Signup_Drop", __FILE__, __LINE__, $logInfo);
                    } else {
                        // 其他的信息不记录，直接out
                        $logInfo['__reason__'] = 'This log does not trigger logging rules';
                        Fend_Log::info(LOG_PREFIX."Consumer_Signup_Drop", __FILE__, __LINE__, $logInfo);
                        continue;
                    }
                    break;
                case "click_count_statistic":
                    // 有那么一些数据就是不存在游戏场次、设备id和学生id，直接滤掉
                    if (!isset($logInfo['_game_round'])
                        || !isset($logInfo['_device_id'])
                    ) {
                        $logInfo ['__reason__'] = 'bad sign_up log format:lose _game_round or _device_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $logInfo);
                        continue;
                    }
                    if(isset($logInfo['_game_id']) && empty($logInfo['_game_id'])) {
                        $logInfo['__reason__'] = 'bad click_count_statistic log format:wrong game_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $logInfo);
                    }
                    $record = array(
                        'game_id' => $logInfo['_game_id'],
                        'round_num' => $logInfo['_game_round'],
                        'interaction_id' => $logInfo['_interaction_id'],
                        'student_click' => $logInfo["_click_count"],
                        'student_click_valid' => $logInfo["_click_count_valid"],
                        'student_click_display' => $logInfo["_display_count"],
                        'device_id' => $logInfo["_device_id"],
                        'student_id' => $logInfo['_student_id'],
                        'student_name' => $logInfo['_student_name'],
                        'student_score' => $logInfo['_student_score'],
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'live_number_id' => $logInfo['live_id'],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'create_time' => $logInfo["ali_time"],
                    );
                    $studentScoreModel = new Model_Monitor_StudentScore();
                    $errorInfo = [];
                    $ret = $studentScoreModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'add or update answer student click_count_statistic fail:' . json_encode($errorInfo);
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $record);
                    }
                    break;

                default:
                    break;
            }
        }
        else if (isset($logInfo['pcstu_version'])) {
            $logInfo['event_name'] = strtoupper($logInfo['event_name']);
            switch ($logInfo['event_name']) {
                case "STU_PLAY_SIGN":
                    // 总学分那边沟通结果：由我们后台日志方过滤
                    $record = array(
                        'game_id' => $logInfo['game_id'],
                        'event_name' => $logInfo['event_name'],
                        'event_time' => $logInfo['event_time'],
                        'event_msec' => $logInfo['event_msec'],
                        'interaction_id' => $logInfo['interaction_id'],
                        'student_id' => $logInfo['student_id'],
                        'student_name' => $logInfo['student_name'],
                        'class_id' => $logInfo["class_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'live_number_id' => $logInfo['live_number_id'],
                        'create_time' => $logInfo["ali_time"],

                        // 0314 新加需求
                        'pcstu_version' => $logInfo["pcstu_version"],
                        'app_subject' => $logInfo["app_subject"], // sci chn eng
                        'tutor_id' => $logInfo["tut_id"],
                        'tutor_name' => $logInfo["tut_name"],
                    );
                    $studentSignModel = new Model_Monitor_StudentSign();
                    $errorInfo = [];
                    $ret = $studentSignModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'add or update answer student signup fail:' . json_encode($errorInfo);
                        Fend_Log::info(LOG_PREFIX."Consumer_Signup", __FILE__, __LINE__, $record);
                    }
                    break;
                case "STU_PLAY_CLICK":
                    // 不存在游戏场次直接滤掉
                    if (!isset($logInfo['round_num'])) {
                        $logInfo ['__reason__'] = 'bad sign_up log format:lose round_num';
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $logInfo);
                        break;
                    }
                    $record = array(
                        'game_id' => $logInfo['game_id'],
                        'event_name' => $logInfo['event_name'],
                        'event_time' => $logInfo['event_time'],
                        'event_msec' => $logInfo['event_msec'],
                        'interaction_id' => $logInfo['interaction_id'],
                        'student_id' => $logInfo['student_id'],
                        'student_name' => $logInfo['student_name'],
                        'class_id' => $logInfo["class_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'live_number_id' => $logInfo['live_number_id'],
                        'create_time' => $logInfo["ali_time"],
                        'round_num' => $logInfo['round_num'],
                        'student_click_valid' => $logInfo["student_click_valid"],

                        // 0314 新加需求
                        'pcstu_version' => $logInfo["pcstu_version"],
                        'app_subject' => $logInfo["app_subject"], // sci chn eng
                        'tutor_id' => $logInfo["tut_id"],
                        'tutor_name' => $logInfo["tut_name"],
                    );
                    $studentScoreModel = new Model_Monitor_StudentScore();
                    $errorInfo = [];
                    $ret = $studentScoreModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'add or update answer student click_count_statistic fail:' . json_encode($errorInfo);
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $record);
                    }
                    break;
                default:
                    break;
            }
        }
        else {
            $logInfo['event_name'] = strtoupper($logInfo['event_name']);
            switch ($logInfo['event_name']) {
                case "STU_PLAY_SIGN":
                    if(isset($logInfo['_game_id']) && empty($logInfo['_game_id'])) {
                        $logInfo['__reason__'] = 'bad sign_up log format:wrong game_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Signup", __FILE__, __LINE__, $logInfo);
                    }
                    // 总学分那边沟通结果：由我们后台日志方过滤
                    $record = array(
                        'game_id' => $logInfo['_game_id'],
                        'event_name' => $logInfo['event_name'],
                        'event_time' => $logInfo['event_time'],
                        'event_msec' => $logInfo['event_msec'],
                        'interaction_id' => $logInfo['_interaction_id'],
                        'student_id' => $logInfo['_student_id'],
                        'student_name' => $logInfo['_student_name'],
                        'student_score' => $logInfo['_student_score'],
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'live_number_id' => $logInfo['live_id'],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'create_time' => $logInfo["ali_time"],
                    );
                    $studentSignModel = new Model_Monitor_StudentSign();
                    $errorInfo = [];
                    $ret = $studentSignModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'add or update answer student signup fail:' . json_encode($errorInfo);
                        Fend_Log::info(LOG_PREFIX."Consumer_Signup", __FILE__, __LINE__, $record);
                    }
                    break;
                case "STU_PLAY_CLICK":
                    // 有那么一些数据就是不存在游戏场次、设备id和学生id，直接滤掉
                    if (!isset($logInfo['_game_round'])
                        || !isset($logInfo['_device_id'])
                    ) {
                        $logInfo ['__reason__'] = 'bad sign_up log format:lose _game_round or _device_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $logInfo);
                        break;
                    }
                    if(isset($logInfo['_game_id']) && empty($logInfo['_game_id'])) {
                        $logInfo['__reason__'] = 'bad click_count_statistic log format:wrong game_id';
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $logInfo);
                    }
                    $record = array(
                        'game_id' => $logInfo['_game_id'],
                        'event_name' => $logInfo['event_name'],
                        'event_time' => $logInfo['event_time'],
                        'event_msec' => $logInfo['event_msec'],
                        'round_num' => $logInfo['_game_round'],
                        'interaction_id' => $logInfo['_interaction_id'],
                        'student_click' => $logInfo["_click_count"],
                        'student_click_valid' => $logInfo["_click_count_valid"],
                        'student_click_display' => $logInfo["_display_count"],
                        'device_id' => $logInfo["_device_id"],
                        'student_id' => $logInfo['_student_id'],
                        'student_name' => $logInfo['_student_name'],
                        'student_score' => $logInfo['_student_score'],
                        'city_id' => $logInfo["city_id"],
                        'city_name' => $logInfo["city_name"],
                        'class_id' => $logInfo["class_id"],
                        'class_name' => $logInfo["class_name"],
                        'client_version' => $logInfo["client_version"],
                        'lecturer_name' => $logInfo["lecturer_name"],
                        'lesson_id' => $logInfo["lesson_id"],
                        'lesson_num' => $logInfo["lesson_num"],
                        'log_source' => $logInfo["log_source"],
                        'live_number_id' => $logInfo['live_id'],
                        'pc_guid' => $logInfo["pc_guid"],
                        'room_name' => $logInfo["room_name"],
                        'time_desc' => $logInfo["time_desc"],
                        'tutor_name' => $logInfo["tutor_name"],
                        'create_time' => $logInfo["ali_time"],
                        'is_sign' => $logInfo["is_sign"],
                    );
                    $studentScoreModel = new Model_Monitor_StudentScore();
                    $errorInfo = [];
                    $ret = $studentScoreModel->add($record, $errorInfo);
                    if (!$ret) {
                        $record ['__reason__'] = 'add or update answer student click_count_statistic fail:' . json_encode($errorInfo);
                        Fend_Log::info(LOG_PREFIX."Consumer_Score", __FILE__, __LINE__, $record);
                    }
                    break;
                default:
                    break;
            }
        }
        return;
    }

    /*
     * 数据预处理
     * */
    private function dataPretreat(& $logInfo)
    {
        if (!empty($logInfo['event_time'])) {
            if (strlen($logInfo['event_time']) > 10) {
                // 保留后面的几位到毫秒数中
                $timeArr = str_split($logInfo['event_time'], 10);
                $event_time = $timeArr[0];
                $msec = $timeArr[1];
            } else {
                $event_time = $logInfo['event_time'];
                $msec = 0;
            }
            $logInfo['event_time'] = $event_time;
            $logInfo['event_msec'] = $msec;
        }
        if (!empty($logInfo['is_sign'])) {
            if ($logInfo['is_sign'] == 'true') {
                $logInfo['is_sign'] = 1;
            } else if ($logInfo['is_sign'] == 'false') {
                $logInfo['is_sign'] = 2;
            } else {
                // 不符合预期的情况，记录日志，找开发
                Fend_Log::info(LOG_PREFIX."Consumer_Playtime", __FILE__, __LINE__,
                    'This log is wrong because of is_sign | ' . json_encode($logInfo));
                $logInfo['is_sign'] = 3;
            }
        }
        if (isset($logInfo['_student_score'])) {
            $logInfo['_student_score'] = str_replace('总学分:', '', $logInfo['_student_score']);
        }
    }

    /*
     * 数据过滤
     * */
    private function logFilter($log)
    {
        if (!isset($log["class_id"]) || $log["class_id"] == "") {
            $log ['__reason__'] = 'bad log format:lose  classid';
            Fend_Log::info(LOG_PREFIX."Consumer", __FILE__, __LINE__, $log);
            return false;
        }
        return true;
    }

}