<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/12/1
 * Time: 上午1:43
 */
class RecoveryData
{
    private $config;
    private $eventArr;
    private $offsetMap;
    private $uniqueMap;
    private $eventFiled;
    private $fieldMap;
    private $missLog;
    private $hostsArr;
    private $logstore;
    private $tableTag;
    private $model;
    private $count;
    const MAX = 5000;


    public function __construct($config,$model)
    {
        $this->config = $config;
        $this->eventArr = [
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
            ],
            'interaction_middle_tutor'=>[
                'SEND_POINT_MANY',
                'SUBJECTIVE_RECORD',
                'INTERACTION_ATTENTION_CLASS_CLOSE',
                'CLASS_RED_MULT',
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
            ],
            'tug_game_start'=>[
                'PLAY_TIME_OPEN',
                'PLAY_TIME_CLOSE',
            ],
            'tug_game_result'=>[
                'PLAY_TIME_TUG'
            ],
            'tug_game_student_score'=>[
                'STU_PLAY_CLICK',
            ],
            'tug_game_student_sign'=>[
                'STU_PLAY_SIGN'
            ],
        ];
        $this->offsetMap = [
            'interaction_middle_major'=>'create_time',
            'interaction_middle_tutor'=>'create_time',
            'interaction_middle_stu'=>'create_time',
            'tug_game_start'=>'create_time',
            'tug_game_result'=>'create_date',
            'tug_game_student_score'=>'create_time',
            'tug_game_student_sign'=>'create_time',
        ];
        $this->uniqueMap = [
            'interaction_middle_major'=>[
                'event_name',
                'interaction_id',
                'event_time',
            ],
            'interaction_middle_tutor'=>[
                'event_name',
                'interaction_id',
                'event_time',
                'class_id',
            ],
            'interaction_middle_stu'=>[
                'event_name',
                'interaction_id',
                'event_time',
                'stu_id',
            ],
            'tug_game_start'=>[
                'action_type',
                'interaction_id',
                'create_time',
            ],
            'tug_game_result'=>[
                'interaction_id',
                'class_id',
                'lesson_num',
                'round_num',
                'create_date',
            ],
            'tug_game_student_score'=>[
                'interaction_id',
                'round_num',
                'create_time',
                'student_id',
            ],
            'tug_game_student_sign'=>[
                'interaction_id',
                'student_id',
                'create_time',
            ],
        ];
        $this->fieldMap = [
            'interaction_id' => 'interactionId',
            'round_num' => '_game_round',
            'create_time' => 'ali_time',
            'student_id' => '_student_id',
        ];
        $this->hostsArr = [
            '60.205.86.38:9223',
        ];
        $this->logstore = [];
        $this->tableTag = [
            'interaction_middle_major'=>'Pull_Ali_Log_Consumer_Major',
            'interaction_middle_tutor'=>'Pull_Ali_Log_Consumer_Tutor',
            'interaction_middle_stu'=>'Pull_Ali_Log_Consumer_Stu',
            'tug_game_start'=>'Pull_Ali_Log_Consumer_Gamestart',
            'tug_game_result'=>'Pull_Ali_Log_Consumer_Gameresult',
            'tug_game_student_score'=>'Pull_Ali_Log_Consumer_Score',
            'tug_game_student_sign'=>'Pull_Ali_Log_Consumer_Signup',
        ];
        $this->count = [];
        $this->model = $model;

        $this->init();
    }

    public function startRecovery($startTime,$endTime) {
        $redis = Fend_Redis::factory();
        $res = $redis->hashGet("aliyun_pull_log","{$startTime}_{$endTime}_{$this->model}",true);
        if(empty($res)) {
            Fend_Log::info('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,'redis is empty!');
            return false;
        }
        $checkInfo = $res['info'];
        foreach ($checkInfo as $table => $oneRes) {
            if(!$this->checkOneRes($this->tableTag[$table],$startTime,$endTime,$oneRes['aliCount'] - $oneRes['dbCount'])) {
                $logstore = $this->getTableLogstore($table);
                $recoveryLog = $this->recovery($startTime,$endTime,$logstore,$table);
                $res['info'][$table]['isHandle'] = true;
                $res['info'][$table]['result'] = count($this->missLog) <= $oneRes['aliCount'] - $oneRes['dbCount'];
                if(empty($res['recovery'])) {
                    $res['recovery'] = $recoveryLog;
                }
                else {
                    $res['recovery'] = array_merge($res['recovery'],$recoveryLog);
                }
            }
        }
        // 将执行结果更新到 redis 中
        $res['judgeLogstoreInfo'] = $this->count;
        $redis->hashSet('aliyun_pull_log',"{$startTime}_{$endTime}_{$this->model}",$res);
    }

    /**
     * 时间换内存 recovery 数据
     * @param $startTime
     * @param $endTime
     * @param $logstore
     * @param $tableName
     */
    public function recovery($startTime, $endTime, $logstore, $tableName)
    {
        $this->missLog = [];
        $endTime = intval(empty($endTime) ? time() : (is_numeric($endTime) ? $endTime : strtotime($endTime)));
        $startTime = intval(empty($startTime) ? $endTime - 3600 : (is_numeric($startTime) ? $startTime : strtotime($startTime)));
        $res = $this->getDbLog($startTime, $endTime,$tableName);
        if($res === false) {
            Fend_Log::info('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,'data too large,please connect to admin');
            return [];
        }
        $this->judgeAliLog($startTime, $endTime, $logstore, $res,$tableName);
        Fend_Log::alarm('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,$this->missLog);
        echo 'Found '.count($this->missLog).' logs in aliyun not exist at mysql,wait second to insert into mysql'.PHP_EOL;
        $aliyun = $this->config['aliyun'];
        $interactionInfo = [];
        if($logstore == 'doubleteacher_analytics_java' || $logstore == 'doubleteacher_analytics_java_debug') {
            foreach ($this->missLog as $item) {
                if(!empty($item['live_number_id']) && !empty($item['lesson_num'])) {
                    $key = $item['live_number_id'] . '__' . $item['lesson_num'];
                }
                else {
                    continue;
                }
                if(!empty($item['live_number_id']) && !empty($item['lesson_num']) && !isset($interactionInfo[$key])) {
                    $eventTime = substr($item['event_time'],0,10);
                    $interactionInfo[$key]['time'] = empty($eventTime) ? $item['ali_time'] : $eventTime;
                    $interactionInfo[$key]['live_number_id'] = $item['live_number_id'];
                    $interactionInfo[$key]['lesson_num'] = $item['lesson_num'];
                }
            }
        }
        $consumer = new $aliyun[$logstore]['consumeClass']();
        $consumer->batchConsume($this->missLog,[]);
        return $interactionInfo;
    }

    private function judgeAliLog($startTime, $endTime, $logstore, & $res,$tableName)
    {
        $client = new Aliyun_Log_Client($this->config['defaultConf']['endpoint'], $this->config['defaultConf']['accessKeyId'], $this->config['defaultConf']['accessKey']);
        $listShardRequest = new Aliyun_Log_Models_ListShardsRequest($this->config['defaultConf']['project'],$logstore);
        $listShardResponse = $client -> listShards($listShardRequest);
        $shardArr = $listShardResponse-> getShardIds();
        foreach($shardArr  as $shardId)
        {
            #对每一个 ShardId，先获取 Cursor
            $getCursorRequest = new Aliyun_Log_Models_GetCursorRequest($this->config['defaultConf']['project'],$logstore,$shardId,null, $startTime);
            $response = $client -> getCursor($getCursorRequest);
            $cursor = $response-> getCursor();
            $count = 100;
            while(true)
            {
                #从 cursor 开始读数据
                $batchGetDataRequest = new Aliyun_Log_Models_BatchGetLogsRequest($this->config['defaultConf']['project'],$logstore,$shardId,$count,$cursor);
                $response = $client -> batchGetLogs($batchGetDataRequest);
                if($cursor == $response -> getNextCursor())
                {
                    break;
                }

                if(!$this->handleOneArr($response,$endTime,$res,$tableName,$logstore)) {
                    break;
                }

                $cursor = $response -> getNextCursor();
            }
        }
    }

    private function handleOneArr(& $response,$endTime,& $res,$tableName,$logstore) {
        $logGroupList = $response -> getLogGroupList();
        foreach($logGroupList as $logGroup)
        {
            $logInfo = array();
            foreach($logGroup -> getLogsArray() as $log)
            {
                $logInfo['ali_time'] = $log->getTime();
                if($logInfo['ali_time'] > $endTime) {
                    return false;
                }

                $this->count[$logstore]++;

                foreach($log -> getContentsArray() as $content)
                {
                    $logInfo[$content->getKey()] = $content->getValue();
                }

                // 校验日志是否存在
                if(! $this->filter($logInfo,$tableName)) {
                    continue;
                }
                $uniqueIndex = $this->getUniqueIndex($logInfo,'aliyun');
                if(!isset($res[$uniqueIndex])) {
                    $this->missLog[$uniqueIndex] = $logInfo;
                }
            }
        }
        return true;
    }

    private function getDbLog($startTime, $endTime, $tableName) {
        $res = [];

        $read = Fend_Read::Factory($tableName, 'statistic')->getModule();
        $countSql = "SELECT count(*) as COUNT_NUM FROM `[TABLENAME]` WHERE `[OFFSET_FIELD]` BETWEEN $startTime and $endTime";
        $countSql = str_replace('[TABLENAME]',$tableName,$countSql);
        $countSql = str_replace('[OFFSET_FIELD]',$this->offsetMap[$tableName],$countSql);
        $resource = $read->query($countSql);
        $countInfo = $read->fetch($resource);
        $count = $countInfo['COUNT_NUM'];

        if($count > 100000) {
            return false;
        }

        $dataSqlTmp = "SELECT * FROM `[TABLENAME]` WHERE `[OFFSET_FIELD]` BETWEEN $startTime and $endTime LIMIT [OFFSET]," . self::MAX;
        $dataSqlTmp = str_replace('[TABLENAME]',$tableName,$dataSqlTmp);
        $dataSqlTmp = str_replace('[OFFSET_FIELD]',$this->offsetMap[$tableName],$dataSqlTmp);
        if( $count < self::MAX ) {
            $dataSql = str_replace('[OFFSET]',0,$dataSqlTmp);
            $query = $read->query($dataSql);
            while ($row = $read->fetch($query)) {
                $uniqueIndex = $this->getUniqueIndex($row);
                if(!isset($res[$uniqueIndex])) {
                    $res[$uniqueIndex] = $row;
                }
                else {
                    $row ['__reason__'] = 'this log has exist,wrong';
                    Fend_Log::error('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,$row);
                }
            }
        }
        else {
            for ($offset = 0; $offset < $count; $offset += self::MAX) {
                $dataSql = str_replace('[OFFSET]',$offset,$dataSqlTmp);
                $query = $read->query($dataSql);
                while ($row = $read->fetch($query)) {
                    $uniqueIndex = $this->getUniqueIndex($row);
                    if(!isset($res[$uniqueIndex])) {
                        $res[$uniqueIndex] = $row;
                    }
                    else {
                        $row ['__reason__'] = 'this log has exist,wrong';
                        Fend_Log::error('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,$row);
                    }
                }
            }
        }
        return $res;
    }

    private function init() {
        foreach ($this->eventArr as $tableName => $events) {
            foreach ($events as $event_name) {
                $this->eventFiled[$event_name] = $this->uniqueMap[$tableName];
            }
        }
        if($this->model == 4) {
            $this->logstore = [
                'doubleteacher_analytics_java' => [
                    'interaction_middle_major',
                    'interaction_middle_tutor',
                    'interaction_middle_stu',
                    'tug_game_start',
                    'tug_game_result',
                ] ,
                'doubleteacher_analytics_pc' => [
                    'tug_game_student_score',
                    'tug_game_student_sign',
                ],
            ];
            $this->count = [
                'doubleteacher_analytics_java' => 0,
                'doubleteacher_analytics_pc' => 0,
            ];
        }
        else if($this->model == 1 || $this->model == 2 || $this->model == 3) {
            $this->logstore = [
                'doubleteacher_analytics_java_debug' => [
                    'interaction_middle_major',
                    'interaction_middle_tutor',
                    'interaction_middle_stu',
                    'tug_game_start',
                    'tug_game_result',
                ] ,
                'doubleteacher_analytics_pc' => [
                    'tug_game_student_score',
                    'tug_game_student_sign',
                ],
            ];
            $this->count = [
                'doubleteacher_analytics_java_debug' => 0,
                'doubleteacher_analytics_pc' => 0,
            ];
        }
        else {
            $this->logstore = [
            ];
        }
    }

    /**
     * 根据 map 获得唯一的 index
     */
    private function getUniqueIndex($log,$source = 'db') {
        $str = '';
        $uniques = $this->eventFiled[$log['event_name']];
        if($source == 'db') {
            if($log['event_time'] == 0 ) {
                $log['event_time'] = '';
            }
            foreach ($uniques as $unique) {
                $str = $str . (string)(empty($log[$unique]) ? '' : $log[$unique]);
            }
        }
        else if ($source == 'aliyun') {
            if(isset($log['_interaction_id'])) {
                $log['interactionId'] = $log['_interaction_id'];
            }
            foreach ($uniques as $unique) {
                if(isset($this->fieldMap[$unique])) {
                    $unique = $this->fieldMap[$unique];
                }
                if($unique == 'event_time') {
                    $str = $str . (string)(empty($log[$unique]) ? '' : substr($log[$unique],0,10));
                }
                else {
                    $str = $str . (string)(empty($log[$unique]) ? '' : $log[$unique]);
                }
            }
        }
        return $str;
    }

    private function getTableLogstore($tableName) {
        foreach ($this->logstore as $logstore => $tables) {
            if(in_array($tableName,$tables)) {
                return $logstore;
            }
        }
        return '';
    }

    /**
     * filter
     */
    private function filter($log,$tableName) {
        if(in_array($log['event_name'],$this->eventArr[$tableName])) {
            return true;
        }
        return false;
    }

    /**
     * 检测一个时间段内是否出现问题
     * @param $tag
     * @param $startTime
     * @param $endTime
     */
    private function checkOneRes($tag,$startTime,$endTime,$missCount) {
        $query = $tag;
        $filter["range"]["x_timestamp"]["gte"] = $startTime;
        $filter["range"]["x_timestamp"]["lte"] = $endTime;
        if($this->model == 4) {
            $indices = "eagleeye_";
        }
        else {
            $indices = "dev_eagleeye_";
        }
        $res = $this->searchBySimpleQuery($indices,$query,$filter,'',$missCount + 1);
        if($missCount <= count($res)) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * copy from xcl
     * @param $indices
     * @param $query
     * @param $filter
     * @param $offset
     * @param $pageSize
     * @param array $order
     * @return array
     */
    private function searchBySimpleQuery($indices, $query, $filter, $offset, $pageSize, $order = array())
    {

        if (empty($pageSize) || $pageSize <= 0) {
            $pageSize = 100;
        }

        if (empty($offset) || $offset <= 0) {
            $offset = 0;
        }

        if ($indices == "") {
            $indices = "eagleeye_";
        }

        $params = [
            'index' => $indices . '*',
            'type' => 'doubleteacher',
            'size' => $pageSize,
            'from' => $offset,
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => [
                            'query_string' => [
                                "query" => $query,
                                'default_operator' => 'and',
                                'fields' => [
                                    "_all"
                                ],
                            ],
                        ],
                    ]
                ],
                'sort' => $order,
            ],

        ];

        if (!empty($filter)) {
            $params["body"]['query']['bool']['filter'] = $filter;
        }

        try {
            $client = Elk_Init::create()->setHosts($this->hostsArr)->build();

            $searchResult = $client->search($params);
            if (isset($searchResult["hits"]["total"])) {
                $total = $searchResult["hits"]["total"];
                $resultList = array();
                foreach ($searchResult["hits"]["hits"] as $item) {
                    $resultList[] = $item["_source"];
                }
                return $resultList;
            }
            return [];

        } catch (Exception $e) {
            Fend_Log::exception('Pull_Ali_Log_Recovery_Db',__FILE__,__LINE__,'msg:' .$e->getMessage().' | backtrace:'.$e->getTraceAsString(). '| code:'.$e->getCode());
            return [];
        }
    }



}