<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/8
 * Time: 上午10:31
 * des：完成日志队列的消费、信号处理等
 */

class ConsumerManager extends ProcessManager
{
    public $configs;

    public $table;

    public $logstore;

    public $logQueue;

    private $processList;

    private $consumer;

    private $permit;

    public function __construct($configs, $logQueue, $swooletable, $logStore, $consumer)
    {
        parent::__construct();

        $this->configs = $configs;

        $this->logQueue = $logQueue;

        $this->table = $swooletable;

        $this->logstore = $logStore;

        $this->processList = [];

        $this->consumer = $consumer;

        $this->permit = true;

    }

    public function checkSelf()
    {
        if (count($this->processList) < $this->configs['aliyun'][$this->logstore]["consumerCount"]) {
            $this->startConsumers();
        }
    }

    public function startAction($params)
    {
        // do noting
        Fend_CliFunc::setProcessName($this->configs['server']['name'],'consume_'.$this->logstore);
    }

    public function doAction($params)
    {
        $_SERVER['REQUEST_TIME'] = time();

        $log = $this->logQueue->pop();
        $this->reflushTime($params['myPid']);

        //if no data sleep 1 sec
        if (!$log) {
            sleep(1);
            return ;
        }

        $logs = [];
        for ($i = 1; $i < BATCHCOUNT && $log; ++$i) {
            $logs[] = $log;
            $log = $this->logQueue->pop();
        }

        // 最后一个不能丢了
        if($log) {
            $logs[] = $log;
        }
        $this->consumer->batchConsume($logs,$params);
    }

    public function stopAction($params)
    {
        $param['msg'] = 'consumer process finish';
        Fend_Log::info(LOG_PREFIX.'At_Logstore_'.$this->logstore.'_Consumer_Finish',__FILE__,__LINE__,$param);
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
            $waitConsume = $this->table->get('wait_consumer');
            if($waitConsume === false) {
                Fend_Log::info(LOG_PREFIX.'Consumer_Wait',__FILE__,__LINE__,'direct finish, killTime:'.time());
                return false;
            }
            else if(time() - $waitConsume['timestamp'] > CONSUMERWAIT) {
                Fend_Log::info(LOG_PREFIX.'Consumer_Wait',__FILE__,__LINE__,'killTime:'.time().' | receive kill singal time:'.$waitConsume['timestamp']);
                return false;
            }
        }
        return true;
    }

    //收到kill信号时
    public function cheanRes($signal)
    {
        if (isset($this->processList[$signal["pid"]])) {
            //del pid count
            $this->table->del(PROPREFIX . $signal["pid"]);

            unset($this->processList[$signal["pid"]]);

            Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At consume_'.$this->logstore.",consumer process id:" . $signal["pid"] . " have been exit signal:" . $signal["signal"] . " code:" . $signal["code"]);
        }
    }

    /*
     * 开启进程
     * */
    private function startConsumers() {
        for ($startCount = count($this->processList); $startCount < $this->configs['aliyun'][$this->logstore]["consumerCount"]; $startCount++) {
            $pid = $this->newProcess();
            if ($pid) {
                $this->reflushTime($pid);
                $this->processList[$pid] = time();
                Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                    'At consume_'.$this->logstore.",Start Consumer Log Process  Pid:" . $pid);
            } else {
                //fail
                Fend_Log::alarm(LOG_PREFIX."Aliyun_Monitor_Error", __FILE__, __LINE__,
                    'At consume_'.$this->logstore.",Fail Consumer Log Process Start " . " reason:" . \swoole_errno() . ":" . \swoole_strerror(swoole_errno()));
            }
        }
    }

    /*
     * 关闭进程
     * */
    public function killConsumers() {
        foreach ($this->processList as $processid => $val) {
            // 使用 kill -15，考虑依据是 monitor 进程的心跳检测执行 kill -9 即可，这里作为收到 kill all process 时的处理不需要强制 kill
            $ret = \swoole_process::kill($processid);
            Fend_Log::info(LOG_PREFIX."Aliyun_Monitor", __FILE__, __LINE__,
                'At consume_'.$this->logstore.",kill the consumer pid:" . $processid . " ret:" . $ret);
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
            "remark" => 'consume_'.$this->logstore
        ]);
    }

    public function isMyProcess($pid)
    {
        return isset($this->processList[$pid]);
    }

    /*
     * 批量处理日志
     * */
//    abstract function batchConsume($logs,$params);

}

