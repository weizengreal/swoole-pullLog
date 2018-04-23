<?php

class Alarm
{
    private $configs = array();

    private $singal = 0;

    public function __construct($option)
    {
        $this->configs = $option;
    }

    function waitExit()
    {
        $this->singal = time();
    }

    public function scanData()
    {
        declare(ticks=1);
        pcntl_signal(SIGTERM, array($this, "waitExit"));

        while (true) {

            if($this->singal != 0) {
                break;
            }

            /**
             * @var Fend_Redis $facRedis
             */
			$facRedis = Fend_Redis::factory();

			/////////
			//get fresh lesson list
			////////
			$nowtime = time();
			$answer = new Model_Monitor_AnswerStatus();
			$bms = new Model_Bms_Classlesson();

			$result = $answer->getArrayByCondition(" create_time >= $nowtime - 3600 * 8 and create_time <=$nowtime ");
			$classList = array();
			foreach ($result as $rs){
				$where = array(
					"class_id" => $rs["class_id"],
					"lesson_id" => $rs["lesson_id"],
				);
				$info = $bms->getInfoByCondition($where, array());
				if ($info) {
					$rs["lesson_num"] = $info["lesson_num"];
					$rs["lesson_date"] = $info["lesson_date"];
					$rs["lesson_start_time"] = $info["lesson_start_time"];
					$rs["lesson_end_time"] = $info["lesson_end_time"];
					$rs["start_time"] = $info["start_time"];
					$rs["end_time"] = $info["end_time"];
					$rs["live_number_id"] = $info["live_number_id"];
					$rs["status"] = $info["status"];
					$rs["class_time_period"] = $info["class_time_period"];
					$rs["classroom_name"] = $info["classroom_name"];
					$rs["teacher_code"] = $info["teacher_code"];
					$rs["tutor_id"] = $info["tutor_id"];
				}

				$classList[] = $rs;
			}

			foreach ($classList as $classInfo) {

                $id = $classInfo["id"];
                //更新标志，如果有修改则为true
                $isupdate = false;

                ////////////////////
                //refresh bind count
                //补充方式，防止意外退出个别绑定信息不刷新
                ////////////////////

                $model = Model_Monitor_AnswerStatus::factory();
                $statusInfo = $model->getInfoById($id);
                if (!$statusInfo) {
                    Fend_Log::alarm(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                        "Alarm get One Fail1 id:" . $id . " found | " . json_encode($classInfo));
                    continue;
                }

                //get bind list from redis
//				$bindInfo = $redis->hGetAll("mn_" . $statusInfo["lesson_id"] . "_" . $statusInfo["class_id"] . "_" . $statusInfo["pc_guid"]);
                $bindInfo = $facRedis->hashGetAll("mn_" . $statusInfo["lesson_id"] . "_" . $statusInfo["class_id"] . "_" . $statusInfo["pc_guid"]);

                $bindCount = 0;
                foreach ($bindInfo as $k => $v) {
                    $bindItem = json_decode($v, true);
                    if ($bindItem["bind"] == 0) {
                        $bindCount++;
                    }
                }

                if ($bindCount != $statusInfo["bind_count"]) {
                    $statusInfo["bind_count"] = $bindCount;
                    $statusInfo["bind_detail"] = json_encode($bindInfo);

                    //if solve but changed will not solve again
                    //status 2->1
                    if ($statusInfo["status"] == 2) {
                        $statusInfo["status"] = 1;
                        Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                            "unbind change solve to not solve : | " . json_encode($statusInfo));
                    }
                    $isupdate = true;
                }

                /////////////////
                //give an alarm befor start lesson
                //status 0-> 1
                ////////////////

                $startTime = is_null($classInfo["start_time"]) || strlen($classInfo["start_time"]) == 0 ? 0 : strtotime($classInfo["start_time"]);

                if ($statusInfo["status"] == 0 && $classInfo["student_count"] > 0
                    && $classInfo["student_count"] > $classInfo["bind_count"]
                    && $startTime > 0 && $startTime - 5 * 60 <= time()
                ) {

                    //flag update status record
                    $isupdate = true;

                    //alarm once
                    $nowtime = time();
                    $statusInfo["status"] = 1;
                    $statusInfo["alarm_status"] = 1;
                    $statusInfo["alarm_time"] = $nowtime;
                    $statusInfo["alarm_hour"] = $nowtime - strtotime(date("Y-m-d 00:00:00", $nowtime));
                    $statusArr = [];

                    // 因为并没有找到校区与 classid 的对应关系，这边的逻辑直接按照 city_id 找到相对应的负责人查出其钉钉号并发送信息；使用like查询因为数据不多问题不大；
//                    $dSql = "select dingnum from `peiyou_device`.`sh_user` a where a.cityid like '%{$statusInfo['city_id']}%'";
//                    $readDb = Fend_Read::Factory('','device');
//                    $dingList = $readDb->query($dSql);
//                    if(count($dingList) == 0) {
//                        Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
//						    "send an Alarm to User:" . $id . "，do not have responsible person | " . json_encode($statusInfo));
//                        continue;
//                    }
//                    foreach ($dingList as $item) {
//                        $to = $item['dingnum'];
//                        $msg = "绑定报警:" . "学生数:" . $classInfo["student_count"] . "，绑定数:" . $classInfo["bind_count"] . "，请老师及时通知和相关班级。调试信息：" . json_encode($statusInfo) . "";
//                        $sendRespone = Api_Dingding::factory()->sendMsg($to, $msg);
//                        if ($sendRespone["errcode"] != 0) {
//                            $sendRespone = Api_Dingding::factory()->sendMsg($to, $msg);
//                            Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
//                                "DingDing Error: | " . json_encode($sendRespone) . " | " . json_encode(array($to, $msg)));
//                        }
//                        // dingding status
//                        $statusArr[$to] = [
//                            'errorcode' => $sendRespone["errcode"],
//                            'errmsg' => $sendRespone["errmsg"],
//                        ];
//                    }

                    // todo 测试时发送到开发人员自己的钉钉号
//                    $to = "zhengwei4@100tal.com";
//                    $msg = "答题器绑定报警:" . "学生数:" . $classInfo["student_count"] . "，绑定数:" . $classInfo["bind_count"] . "，请老师及时通知和相关班级。调试信息：" . json_encode($statusInfo) . "";
//                    $sendRespone = Api_Dingding::factory()->sendMsg($to, $msg);
//                    if ($sendRespone["errcode"] != 0) {
//                        $sendRespone = Api_Dingding::factory()->sendMsg($to, $msg);
//                        Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
//                            "DingDing Error: | " . json_encode($sendRespone) . " | " . json_encode(array($to, $msg)));
//                    }
//                    //dingding status
//                    $statusInfo["dingding_status"] = $sendRespone["errcode"] . ":" . $sendRespone["errmsg"];
                    $statusInfo["dingding_status"] = json_encode($statusArr,JSON_UNESCAPED_UNICODE);

                    Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                        "send an Alarm to User:" . $id . " | " . json_encode($statusInfo));
                }

                ///////////
                /// 检测问题是否解决
                /// status 1->2
                ///////////

                if ($statusInfo["status"] == 1 && $statusInfo["bind_count"] == $statusInfo["student_count"]) {
                    $nowtime = time();
                    $statusInfo["status"] = 2; //alarmed finished
                    $statusInfo["solve_time"] = $nowtime;
                    //$statusInfo["solve_hour"] = $nowtime - strtotime(date("Y-m-d 00:00:00", $nowtime));

                    //flag update status record
                    $isupdate = true;

                    Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                        "Solve an Alarm User:" . $id . " | " . json_encode($statusInfo));
                }

                if ($isupdate) {
                    //update
                    $errorInfo = array();
                    $ret = $model->add($statusInfo, $errorInfo);
                    if (!$ret) {
                        Fend_Log::alarm(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                            "Alarm Update alarm status Fail: $ret | " . json_encode($errorInfo) . " | " . json_encode($statusInfo));
                        continue;
                    }
                }


            }
//			$redis->close();

            sleep(1);

        }
    }

    /*
     * 向指定的钉钉号发送报警信息
     * */
    private function sendDing($toUser,$msg) {
        $sendRespone = Api_Dingding::factory()->sendMsg($toUser, $msg);
        if ($sendRespone["errcode"] != 0) {
            $sendRespone = Api_Dingding::factory()->sendMsg($toUser, $msg);
            Fend_Log::info(LOG_PREFIX."Monitor_alarm", __FILE__, __LINE__,
                "DingDing Error: | " . json_encode($sendRespone) . " | " . json_encode(array($toUser, $msg)));
        }
        //dingding status
        $statusInfo["dingding_status"] = $sendRespone["errcode"] . ":" . $sendRespone["errmsg"];
    }


}
