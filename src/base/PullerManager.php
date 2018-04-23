<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/8
 * Time: 上午10:31
 * des：完成信号处理，日志拉取到指定队列
 */

class PullerManager extends ProcessManager
{
    public $configs;

    public $table;

    public $pullOption;

    public $logstore;

    public $logQueue;

    private $shardList;

    private $processList;

    private $cursor;

    private $client;

    private $puller;

    private $permit;

    public function __construct($configs, $logQueue, $swooletable, $logStore, $puller)
    {
        parent::__construct();

        $this->configs = $configs;

        $this->logQueue = $logQueue;

        $this->table = $swooletable;

        $this->pullOption = $configs['aliyun'][$logStore];

        if(!isset($configs['aliyun'][$logStore]['customConf'])) {
            $this->pullOption['customConf'] = $this->configs['defaultConf'];
        }
        else {
            $customConf = array_merge($this->configs['defaultConf'],$configs['aliyun'][$logStore]['customConf']);
            $this->pullOption['customConf'] = $customConf;
        }

        $this->shardList = [];

        $this->processList = [];

        $this->logstore = $logStore;

        $this->cursor = null;

        $this->client = null;

        $this->puller = $puller;

        $this->permit = true;
    }

    /**
     * 刷新 shardId
     */
    public function reflushShard($pid) {
        $this->reflushTime($pid);
        try {
            //init
            $client = new Aliyun_Log_Client($this->pullOption["customConf"]["endpoint"], $this->pullOption["customConf"]["accessKeyId"],
                $this->pullOption["customConf"]["accessKey"], $this->pullOption["customConf"]["token"]);

            $listShardRequest = new Aliyun_Log_Models_ListShardsRequest($this->pullOption["customConf"]["project"], $this->logstore);
            $shardArray = $client->listShards($listShardRequest);
            $shardIdArray = $shardArray->getShardIds();
            $this->reflushTime($pid);

            Fend_Log::debug( LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";get Shard List:" . implode(",", $shardIdArray));

            //alarm when nothing get from aliyun
            if (count($shardIdArray) == 0) {
                //alarm
                Fend_Log::error( LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                    'At pull_'.$this->logstore.";get Shard List Fail...");
                return;
            }

            foreach ($shardIdArray as $shardId) {
                $this->table->set(SHARDPREFIX.$this->logstore.'_'.$shardId,[
                    'timestamp' => time(),
                    'remark' => $shardId,
                ]);
            }
        } catch (Exception $e) {
//            echo $e->getPrevious().PHP_EOL;
            Fend_Log::error( LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";get Shard List Fail...,exception msg:".$e->getMessage());
            return;
        }
    }

    /*
     * 用于monitor轮询
     * */
    public function checkSelf()
    {
        // 取得来源于 check 进程刷新的 shard 列表
        $shards = $this->table->getListByPrefix(SHARDPREFIX.$this->logstore);
        if(count($shards) == 0) {
            Fend_Log::alarm(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore." | shard list empty!");
            return ;
        }
        $shardIdArray = [];
        foreach ($shards as $shardItem) {
            if(time() - $shardItem['timestamp'] > $this->configs['server']['processtimeout']) {
                Fend_Log::alarm(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                    'At pull_'.$this->logstore." | shardList ten seconds without refresh");
                return ;
            }
            $shardIdArray [] = intval($shardItem['remark']);
        }

        if (count(array_diff($shardIdArray, $this->shardList)) > 0) {

            $this->shardList = $shardIdArray;

            Fend_Log::info( LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";ShardID changed respawn for balance..");

            $this->restartAll();
            return ;
        }

        //make sure all ali yun pull log process started
        foreach ($this->shardList as $shardId) {
            $pid = array_search($shardId,$this->processList);
            if (!$pid || Fend_CliFunc::ifrun($pid) == 0) {
                //alarm
                Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                    'At pull_'.$this->logstore.";pull log process:" . $shardId . " not run .. restart");

                $this->restartAll();
                return ;
            }
        }
    }

    public function startAction($param)
    {
        $pid = $param['myPid'];
        $shardId = $param['shardId'];
        Fend_CliFunc::setProcessName($this->configs['server']['name'],'pull_'.$this->logstore.$shardId);

        $shardOffset = $this->table->getField($this->logstore . $shardId,'timestamp');
//        $shardOffset = $shardArr['timestamp'];
        if(!$shardOffset) {
            Fend_Log::info(LOG_PREFIX.'Puller_No_Time',__FILE__,__LINE__,'没有找到recordTable的时间戳，默认使用四小时前的时间戳开始拉取');
            $shardOffset = time() - 3600 * 4;
            $this->reflushShardTime($shardId,$shardOffset);
        }
        else {
            // 回退 20s ，目的是保证数据不丢失
            $shardOffset = $shardOffset - 20;
        }
        $this->reflushTime($pid);

        $this->client = new Aliyun_Log_Client($this->pullOption["customConf"]["endpoint"], $this->pullOption["customConf"]["accessKeyId"],
            $this->pullOption["customConf"]["accessKey"], $this->pullOption["customConf"]["token"]);

        try {
            $listShardRequest = new Aliyun_Log_Models_ListShardsRequest($this->pullOption["customConf"]["project"], $this->logstore);
            $listShardResponse = $this->client->listShards($listShardRequest);
            foreach ($listShardResponse->getShardIds() as $_shardId) {
                //ignore the not same shardid
                if ($_shardId != $shardId) {
                    continue;
                }
                $getCursorRequest = new Aliyun_Log_Models_GetCursorRequest($this->pullOption["customConf"]["project"], $this->logstore, $_shardId, null, $shardOffset);
                $response = $this->client->getCursor($getCursorRequest);
                // 初始化光标所在位置
                $this->cursor = $response->getCursor();
                $this->reflushTime($pid);
            }//foreach shard
        } catch (Exception $e) {
            Fend_Log::exception(LOG_PREFIX."Puller_StartAction", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";Curl Exception:" . $e->getCode() . " | " . $e->getMessage() . PHP_EOL . $e->getTraceAsString());
            $this->permit = false;
        }
    }

    /*
     * 一次最简的curl情况原子操作
     * channel 的 push 操作使用 while 锁机制
     * offset 只有在当前日志成功处理之后才允许更新
     * 在逻辑端中的各个需要竞争资源的代码段后刷新心跳时间戳以应对极限条件
     * */
    public function doAction($param)
    {
        // 每次抓取最大 MAXPULLCOUNT 条，所以每次请求之前先 sleep 1000 微妙
        usleep(1000);

        $pid = $param['myPid'];
        $shardId = $param['shardId'];
        $shardOffset = $this->table->getField($this->logstore . $shardId,'timestamp');
        $batchGetDataRequest = new Aliyun_Log_Models_BatchGetLogsRequest($this->pullOption["customConf"]["project"], $this->logstore, $shardId, MAXPULLCOUNT, $this->cursor);
        $response = $this->client->batchGetLogs($batchGetDataRequest);
        // curl 成功了，更新时间戳保证进程稳定，极限条件
        $this->reflushTime($pid);

        // TODO:: arrive end 可以和业务场景相结合，正常情况下为 debug 日志，如果在超过一定时间且处于应该上课的时间段内就应该记录日志了
        //arrive end
        if ($this->cursor == $response->getNextCursor()) {
            $this->reflushTime($pid);
            //debug info
            Fend_Log::debug(LOG_PREFIX."Pull_Process", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";Arrive Fetch End..." . $shardId . " offset:" . $shardOffset . " date:" . date("Y-m-d H:i:s", $shardOffset));
            sleep(1);
        }

        $logGroupList = $response->getLogGroupList();
        foreach ($logGroupList as $logGroup) {

            foreach ($logGroup->getLogsArray() as $log) {
                $this->reflushTime($pid);

                // get upload to aliyun log timestamp
                $key = $log->getTime();
                $logInfo = array("ali_time" => $key);

                // get one log
                foreach ($log->getContentsArray() as $content) {
                    $logInfo[$content->getKey()] = $content->getValue();
                }

                // push to channel
                if ($this->puller->filter($logInfo)) {
                    while (!$this->logQueue->push($logInfo)) {
                        // if push fail, alarm
                        Fend_Log::alarm(LOG_PREFIX."Pull_Process", __FILE__, __LINE__,
                            'At pull_'.$this->logstore.";Log Queue is Full...");

                        // 设置心跳到十秒之后
                        $this->reflushTime($pid,time() + $this->configs['server']['processtimeout']);
                        sleep(10);
                    }
//                    if(isset($logInfo['event_name'])) {
//                        echo 'push:aliTime:'.date("Y-m-d H:i:s",$logInfo['ali_time']).'|push into channel,shard id:'.$shardId.'|event_name:'.$logInfo['event_name'].PHP_EOL;
//                    }
                }

                // handle this log successful，update last log time
                if ($logInfo["ali_time"] > $shardOffset) {
                    //store to table
                    $this->reflushShardTime($shardId,$logInfo["ali_time"]);
                }
            }
        }
        $this->cursor = $response->getNextCursor();
    }

    public function stopAction($param)
    {
        $param['msg'] = 'puller process finish';
        Fend_Log::info(LOG_PREFIX.'Puller_Finish',__FILE__,__LINE__,$param);
    }

    /*
     * 重写进程检测函数
     * 处理来自 monitor 进程的 kill signal
     * */
    public function check()
    {
        if(! $this->permit) {
            return false;
        }
        $check = parent::check();
        if(!$check) {
            Fend_Log::info(LOG_PREFIX.'Puller_Finish',__FILE__,__LINE__,'direct finish, killTime:'.time());
            return false;
        }
        return true;
    }

    public function cheanRes($signal)
    {
        if (isset($this->processList[$signal["pid"]])) {
            //del pid count
            $this->table->del(PROPREFIX . $signal["pid"]);

            unset($this->processList[$signal["pid"]]);

            Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";process id:" . $signal["pid"] . " have been exit signal:" . $signal["signal"] . " code:" . $signal["code"]);
        }
    }

    /*
     * 重启全部进程函数
     * */
    private function restartAll() {
        Fend_Log::info( LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
            'At pull_'.$this->logstore.";Restarting the Pull Log Process...");
        $this->killPullers();
        $this->startPullers();
    }

    /*
     * 开启进程
     * */
    private function startPullers() {
        foreach ($this->shardList as $key => $shardID) {
            $pid = $this->newProcess([
                'shardId'=>$shardID
            ]);
            if($pid) {
                // 记录下当前进程信息到 swoole_table
                $this->reflushTime($pid);
                $this->processList[$pid] = $shardID;
                Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                    'At pull_'.$this->logstore.",Start Puller Log Process  Pid:" . $pid);
            }
            else {
                Fend_Log::alarm(LOG_PREFIX.'Aliyun_Monitor_Error',__FILE__,__LINE__,'申请一个进程失败');
            }
        }
    }

    /*
     * kill pullers
     * */
    public function killPullers() {
        foreach ($this->processList as $processid => $shardId) {
            // 使用 kill -15，考虑依据是 monitor 进程的心跳检测执行 kill -9 即可，这里作为收到 kill all process 时的处理不需要强制 kill
            $ret = \swoole_process::kill($processid);
            Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At pull_'.$this->logstore.";kill the pid:" . $processid . " ret:" . $ret);
        }
    }

    /*
     * 更新当前进程的时间戳
     * */
    private function reflushTime($pid,$time = 0) {
        if($time == 0) {
            $time = time();
        }
        $this->table->set(PROPREFIX . $pid, [
            "timestamp" => $time,
            "remark" => 'pull_'.$this->logstore
        ]);
    }

    public function isMyProcess($pid)
    {
        return isset($this->processList[$pid]) || $this->table->get(SHARDPREFIX.$pid) !== false;
    }

    /*
     * 更新某 logstore 中某 shard 的时间戳
     * */
    private function reflushShardTime($stardId,$time = 0,$remark = null) {
        if($time == 0) {
            $time = time();
        }
        if(empty($remark)) {
            $remark = $this->logstore;
        }
        $this->table->set($this->logstore . $stardId, [
            "timestamp" => $time,
            "remark" => $remark
        ]);
    }


    /*
     * 过滤逻辑
     * */
//    abstract function filter($log) ;

}