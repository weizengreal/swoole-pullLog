<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/8
 * Time: 上午10:35
 */

class LogMonitor
{

    // 所有配置
    private $config;

    // http 服务器
    private $server;

    // 服务名称
    private $serverName;

    // memory table
    private $memoryTable;

    // 日志队列
    private $logQueues;

    // 拉取类资源集合
    private $pullers;

    // 消费类资源集合
    private $consumers;

    // kill signle
    private $killSignal;

    // dump channel data path
    private $path;

    // check kill signal
    private $checkSignal;

    public function __construct($config)
    {
        $this->config = $config;
        $this->serverName = $this->config['server']['name'];
        $this->server = new swoole_http_server($this->config['server']['host'], $this->config['server']['port']);

        // 初始化 swoole_table 用于 dump offset
        $cols = [
            [
                'key' => 'timestamp',
                'type' => \swoole_table::TYPE_INT,
                'len' => 8,
            ],
            [
                'key' => 'remark',
                'type' => \swoole_table::TYPE_STRING,
                'len' => 64,
            ],
        ];
        // TODO:: memoryTable 路径问题
        $webroot = $GLOBALS['_cfg']['sys_rootdir'];
        $this->memoryTable = new Fend_MemoryTable($cols,$webroot.$this->config['server']['recordpath'],$this->config['server']['tablesize']);
        $this->path = $webroot.$this->config['server']['channelpath'];
        $this->killSignal = 0;
        $this->consumers = [];
        $this->pullers = [];
        $this->logQueues = [];
        $this->checkSignal = 0;

        $this->init();
    }

    /*
     * 初始化操作
     * */
    public function init()
    {
        // http 服务器相关的初始化操作
        $this->server->set($this->config['swoole']);

        // http 服务器 manager 进程
        $this->server->on('ManagerStart', array($this, 'onManagerStart'));
        $this->server->on('ManagerStop', array($this, 'onManagerStop'));

        // http 服务器 work 进程管理
        $this->server->on('WorkerStart', array($this, 'onWorkerStart'));
        $this->server->on('WorkerStop', array($this, 'onWorkerStop'));
        $this->server->on('WorkerError', array($this, 'onWorkerError'));
        $this->server->on('Request', array($this, 'onRequest'));

        // task 相关事件配置
//        $this->server->on('Task', array($this, 'onTask'));
//        $this->server->on('Finish', array($this, 'onFinish'));

        // http 服务器开启和关闭
        $this->server->on('Shutdown', array($this, 'onShutdown'));
        $this->server->on('Start', array($this, 'onStart'));

        Fend_LogAgent::setDumpLogMode(2);
        Fend_LogAgent::setLogPath($this->config['server']['logpath']);

        // 初始化 puller 和 consumer 进程，同时初始化对应的 channel
        $aliyunConf = $this->config['aliyun'];
        foreach ($this->config['vaildStore'] as  $logStore) {
            $this->logQueues[$logStore] = new \swoole_channel($this->config['defaultConf']['channelSize']);
            $this->pullers[$logStore] = new PullerManager($this->config,$this->logQueues[$logStore],$this->memoryTable,$logStore,new $aliyunConf[$logStore]['pullClass']());
            $this->consumers[$logStore] = new ConsumerManager($this->config,$this->logQueues[$logStore],$this->memoryTable,$logStore,new $aliyunConf[$logStore]['consumeClass']());
        }

        // 初始化 check process
        $this->server->addProcess(new \swoole_process(function () {
            Fend_CliFunc::setProcessName($this->serverName,'check');
            $this->checkShards();
        }));

        // 初始化 monitor 函数
        $this->server->addProcess(new \swoole_process(function () {
            Fend_CliFunc::setProcessName($this->serverName,'monitor');
            $this->monitor();
        }));

        // Alarm 只有PC存在时才开启
        if(in_array('doubleteacher_analytics_pc',$this->config['vaildStore'])) {
            $this->server->addProcess(new \swoole_process(function () {
                Fend_CliFunc::setProcessName($this->serverName,'alarm');
                $alarm = new Alarm($this->config);
                $alarm->scanData();
            }));
        }

        // 初始化日志存储 logAgent
        $this->server->addProcess(new \swoole_process(function () {
            Fend_CliFunc::setProcessName($this->serverName,'logagent');
            Fend_LogAgent::threadDumpLog();
        }));
    }


    /**
     * check process ，用于刷新所有 logStore 的 shardList
     *
     */
    public function checkShards() {
        // 监控
        declare(ticks=1);
        pcntl_signal(SIGTERM, function ($signal) {
            Fend_Log::info(LOG_PREFIX."Check_Process_Signal", __FILE__, __LINE__,
                "check process will die...signal:" . $signal);
            $this->checkSignal = time();
        });
        $pid = getmypid();
        if(PIDMAX != -1 && $pid > PIDMAX) {
            Fend_Log::error(LOG_PREFIX."Aliyun_Process_Manager", __FILE__, __LINE__,
                'get my pid error!getmypid value is:'.$pid);
            exit();
        }
        $this->memoryTable->set(SHARDPREFIX.$pid,[
            'timestamp' => time(),
            'remark' => 'store_for_check',
        ]);
        /**
         * @var PullerManager $pullObj
         */
        while ($this->checkSignal == 0) {
            foreach ($this->pullers as $pullObj) {
                $pullObj->reflushShard($pid);
            }
            sleep(1);
        }

        // 进程即将结束，释放自己占用的资源
        $this->memoryTable->del(PROPREFIX.getmypid());
    }

    /**
     * monitor 进程
     */
    public function monitor()
    {
        echo 'Monitor Start,now:'.date('Y-m-d G:i:s').PHP_EOL;
        declare(ticks=1);
        pcntl_signal(SIGTERM, function ($signal) {
            Fend_Log::info(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                "monitor will kill the all process...signal:" . $signal);
            Fend_CliFunc::setProcessName($this->serverName,'monitor_waiting');
            $this->killSignal = time();
        });
        pcntl_signal(SIGCHLD, array($this, "childWait"));


        $this->initChannel();
        $selfPid = getmypid();
        /**
         * @var PullerManager $pullObj
         * @var ConsumerManager $consumerObg
         */
        while ($this->killSignal ==0) {

            //check all pull log process list
            foreach ($this->pullers as $pullObj) {
                $pullObj->checkSelf();
            }

            //check all consumer process
            foreach ($this->consumers as $consumerObg) {
                $consumerObg->checkSelf();
            }

            $hearts = $this->memoryTable->getListByPrefix(PROPREFIX);
            foreach ($hearts as $k => $v) {
                $pid = substr($k, strlen(PROPREFIX));

                //is not this monitor create?
                if (!$this->isConsumerProcess($pid) && !$this->isPullProcess($pid)) {

                    if(! $this->memoryTable->del($k)) {
                        Fend_Log::error(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                            "delete memory table key error");
                    }
                    $ret = swoole_process::kill($pid, SIGKILL);

                    Fend_Log::exception(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                        "process pid not Current Monitor create:" . $pid . " ret:" . $ret . " type:" . $v["remark"]);
                    continue;
                }

                //time out decide
                $timestamp = $v["timestamp"];
                if (time() - $timestamp > $this->config["server"]["processtimeout"]) {

                    if(! $this->memoryTable->del($k)) {
                        Fend_Log::error(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                            "delete memory table key error");
                    }
                    $ret = swoole_process::kill($pid, SIGKILL);

                    Fend_Log::exception(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                        "process heartBeat Timeout pid:" . $pid . " ret:" . $ret . " time:" . $timestamp . " type:" . $v["remark"]);
                    continue;
                }
            }

            $this->dumpTableRecord();
            $this->memoryTable->set('recent_monitor_time',[
                'timestamp' => time(),
                'remark' => $selfPid
            ]);

            sleep(1);
        }

        $this->finishMonitor();

        // wait all children process
        $count = 0;
        while($count < 15){
            while ($childInfo = swoole_process::wait(false)) {
                if ($childInfo){

                    //ali yun log pull
                    foreach ($this->pullers as $pullObj) {
                        $pullObj->cheanRes($childInfo);
                    }

                    foreach ($this->consumers as $consumerObg) {
                        $consumerObg->cheanRes($childInfo);
                    }

                    // warning
                    Fend_Log::info(LOG_PREFIX."Monitor_Main_Pull", __FILE__, __LINE__,
                        "process id:" . $childInfo["pid"] . " have been exit signal:" . $childInfo["signal"] . " code:" . $childInfo["code"]);
                }
            }

            $hearts = $this->memoryTable->getListByPrefix(PROPREFIX);
            foreach ($hearts as $k => $v) {
                $pid = substr($k, strlen(PROPREFIX));
                //time out decide
                $timestamp = $v["timestamp"];
                if (time() - $timestamp > $this->config["server"]["processtimeout"]) {

                    $this->memoryTable->del($k);
                    $ret = swoole_process::kill($pid, SIGKILL);

                    Fend_Log::exception(LOG_PREFIX."Monitor", __FILE__, __LINE__,
                        "process heartBeat Timeout pid:" . $pid . " ret:" . $ret . " time:" . $timestamp . " type:" . $v["remark"]);
                    continue;
                }
            }
            if(count($hearts) == 0) {
                break;
            }
            $count ++;
            sleep(1);
        }

        // kill 所有 consumer 之后，将 channel 中所有数据全部 dump 下来
        $this->dumpChannel();

        echo 'Monitor Finish,now:'.date('Y-m-d G:i:s').PHP_EOL;
    }

    function childWait()
    {
        //release the exit process
        while ($ret = \swoole_process::wait(false)) {

            /**
             * ali yun log pull
             * @var PullerManager $pullObj
             * @var ConsumerManager $consumerObg
             */
            foreach ($this->pullers as $pullObj) {
                $pullObj->cheanRes($ret);
            }

            foreach ($this->consumers as $consumerObg) {
                $consumerObg->cheanRes($ret);
            }

            // warning
            Fend_Log::info(LOG_PREFIX."Monitor_Main_Pull", __FILE__, __LINE__,
                "process id:" . $ret["pid"] . " have been exit signal:" . $ret["signal"] . " code:" . $ret["code"]);
            return true;
        }
    }

    private function finishMonitor()
    {
        //kill all pull process
        foreach ($this->pullers as $logstore => $pullObj) {
            $pullObj->killPullers();
        }

        $this->memoryTable->set('wait_consumer',array(
            'timstamp' => time(),
            'remark' => 'waitConsumer'
        ));
        $this->dumpTableRecord();

        // 保证 puller 进程先收到kill信号并及时停止
        sleep(1);

        foreach ($this->consumers as $logstore => $consumerObg) {
            $consumerObg->killConsumers();
        }

        Fend_Log::info(LOG_PREFIX.'Monitor_Handle_Kill',__FILE__,__LINE__,'receive timestamp:'.(string)$this->killSignal);

        return;
    }

    private function initChannel() {
        // 处理之前时间的某个 channel 日志的 dump
        Fend_Log::info(LOG_PREFIX."Server_Init", __FILE__, __LINE__,"Log Monitor Recovery Old Records...,Wait Seconds!");
        $fileArr = [];
        foreach ($this->logQueues as $storeIndex => $queue) {
            $fileName = $this->path.$storeIndex.'.db';
            if(! file_exists($fileName)) {
                continue ;
            }
            $fileArr[] = $fileName;
            $handle = fopen($fileName, 'r');
            while(!feof($handle)){
                $logStr = fgets($handle);
                $logs = json_decode($logStr , true);
                if(empty($logs)) {
                    Fend_Log::error(LOG_PREFIX."Server_Init", __FILE__, __LINE__,
                        "logStr json decode error!logStr:".$logStr);
                    continue ;
                }
                foreach ($logs as $log) {
                    $ret = $queue->push($log);
                    if(!$ret) {
                        // 留个坑，如果多次出现闪退，或者在下一次启动时改小了channel的大小等可能会造成数据量过大！
                        // 理论上是不可能失败，如果失败记录错误日志并退出，不允许启动
                        Fend_Log::error(LOG_PREFIX."Server_Init", __FILE__, __LINE__,
                            "swoole channel memory too small，");
                        exit;
                    }
                }
                usleep(1000);
            }
            fclose($handle);
        }
        // 只有当 init 完全成功才会删除 dump 日志脚本
        foreach ($fileArr as $fileName) {
            unlink($fileName);
        }
    }

    private function dumpChannel() {
        $stats = [];
        /**
         * @var swoole_channel $channelItem
         */
        foreach ($this->logQueues as $index => $channelItem) {
            $stats[$index] = $channelItem->stats();
        }
        $stats['__reason__'] = 'Log Monitor Dump logs...,Wait Seconds!';
        Fend_Log::info(LOG_PREFIX."Server_Stoping", __FILE__, __LINE__,$stats);
        // 遍历并 dump 所有数据，一个 chnnel 内存为100MB，大约可容纳10W条数据，一次 dump DUMPCOUNT（10000条）数据
        $count = 0;
        foreach ($this->logQueues as $storeIndex => $channelItem) {
            $logs = [];
            $fileName = $this->path.$storeIndex.'.db';
            $log = $channelItem->pop();
            while($log) {
                $logs[] = $log;
                $count ++;
                $log = $channelItem->pop();
                if($count >= DUMPCOUNT) {
                    file_put_contents($fileName,json_encode($logs,JSON_UNESCAPED_UNICODE).PHP_EOL,FILE_APPEND);
                    $logs = [];
                    $count = 0;
                    // sleep 0.1s
                    usleep(1000);
                }
            }
            if(! empty($logs)) {
                file_put_contents($fileName,json_encode($logs,JSON_UNESCAPED_UNICODE).PHP_EOL,FILE_APPEND);
            }
        }
        $logs = null;
    }

    private function dumpTableRecord() {
        $excludekey = [
            'log_level',
            'debug',
            'wait_consumer',
            'recent_monitor_time',
        ];
        $excluedePrefix = [
            PROPREFIX,
            SHARDPREFIX,
        ];
        $this->memoryTable->dumpTableRecord($excludekey,$excluedePrefix);
    }

    // 遍历以确认该pid是否为所有pull的进程
    private function isPullProcess($pid) {
        /**
         * @var PullerManager $pullObj
         */
        foreach ($this->pullers as $pullObj) {
            if($pullObj->isMyProcess($pid)) {
                return true;
            }
        }
        return false;
    }

    // 遍历所有 consumer 的process以确定是否为当前的pid
    private function isConsumerProcess($pid) {
        /**
         * @var ConsumerManager $consumerObg
         */
        foreach ($this->consumers as $consumerObg) {
            if($consumerObg->isMyProcess($pid)) {
                return true;
            }
        }
        return false;
    }

    // start
    public function start()
    {
        $this->server->start();
    }


    public function onManagerStart(\swoole_server $serv)
    {
        Fend_CliFunc::setProcessName($this->serverName, 'manager');
    }

    public function onManagerStop(\swoole_server $serv)
    {
        echo 'Manager Stoped...,now:'.date('Y-m-d G:i:s').PHP_EOL;
    }

    public function onStart(\swoole_server $server)
    {
        Fend_CliFunc::setProcessName($this->serverName, "master");
        echo "MasterPid = $server->master_pid".PHP_EOL;
        echo "ManagerPid = $server->manager_pid".PHP_EOL;
        echo "Swoole version is [" . SWOOLE_VERSION . "]".PHP_EOL;
    }

    public function onRequest(\swoole_http_request $request, \swoole_http_response $response)
    {
        // 实现一些对阿里云日志拉取的 http 接口
        $response->header('Content_Type', 'application/json; charset=utf-8');
        $response->status(200);

        $params = isset($request->post) ? $request->post : array();
        $uri = rtrim($request->server["request_uri"], "/");

        if ($uri == "/ping") {
            $response->end("pong");
            return;
        }

        //status
        if ($uri == "/status") {

            $table = $this->memoryTable->getList();
            $logQueueList = array();
            foreach ($this->logQueues as $index => $channelItem) {
                $logQueueList[$index] = $channelItem->stats();
            }
            $response->end(json_encode([
                "server" => $this->server->stats(),
                "logqueue" => $logQueueList,
                "syslogqueue" => Fend_LogAgent::getQueueStat(),
                "table" => $table,
            ]));
            return;
        }

        // get logQueue's status
        if ($uri == "/logqueueStatus") {
            $stats = [];
            foreach ($this->logQueues as $index => $channelItem) {
                $stats[$index] = $channelItem->stats();
            }
            $response->end(json_encode($stats,JSON_UNESCAPED_UNICODE));
            return;
        }

        // get shard status
        if ($uri == "/shardStatus") {
            $response->end(json_encode($this->memoryTable->getListByPrefix(SHARDPREFIX),JSON_UNESCAPED_UNICODE));
            return;
        }

        // 方便调试  TODO
        if($this->config['server']['name'] != 'pull_log_dev') {
            if(! Fend_CliFunc::checkToken($params,$GLOBALS['_cfg']['aliyun_salt'])) {
                $response->end(Fend_CliFunc::buildResponseResult(8,'you don\'t have permission'));
                return ;
            }
        }

        $response->end('hello world,I love it!you know for data!');
    }


    public function onWorkerStart(\swoole_server $server, $worker_id)
    {
        if (!$server->taskworker) {
            //worker
            Fend_CliFunc::setProcessName($this->serverName, "worker");
        } else {
            //task
            Fend_CliFunc::setProcessName($this->serverName, "task");
        }
    }

//    public function onTask(\swoole_server $server, $task_id, $from_id, $data) {
//
//    }
//
//    public function onFinish(\swoole_server $server, $task_id, $data) {
//
//    }


    public function onWorkerError(\swoole_server $server, $worker_id, $worker_pid, $exit_code)
    {
    }

    public function onWorkerStop(\swoole_server $server, $worker_id)
    {
    }

    public function onShutdown(\swoole_server $server)
    {
        // dump swoole_table 以保存阿里云offset
        $this->dumpTableRecord();

        // dump logAgent 中的日志
        Fend_LogAgent::flushChannel();

        echo 'Server Shutdown,Master Exit!now:'.date('Y-m-d G:i:s').PHP_EOL;
    }


}


function serverHandleFatal()
{
    $error = error_get_last();
    if (isset($error['type'])) {
        switch ($error['type']) {
            case E_ERROR :
            case E_PARSE :
            case E_CORE_ERROR :
            case E_COMPILE_ERROR :
                $message = $error['message'];
                $file = $error['file'];
                $line = $error['line'];
                $log = "$message ($file:$line)\nStack trace:\n";
                $trace = debug_backtrace();
                foreach ($trace as $i => $t) {
                    if (!isset($t['file'])) {
                        $t['file'] = 'unknown';
                    }
                    if (!isset($t['line'])) {
                        $t['line'] = 0;
                    }
                    if (!isset($t['function'])) {
                        $t['function'] = 'unknown';
                    }
                    $log .= "#$i {$t['file']}({$t['line']}): ";
                    if (isset($t['object']) and is_object($t['object'])) {
                        $log .= get_class($t['object']) . '->';
                    }
                    $log .= "{$t['function']}()\n";
                }
                if (isset($_SERVER['REQUEST_URI'])) {
                    $log .= '[QUERY] ' . $_SERVER['REQUEST_URI'];
                }
                Fend_log::exception(LOG_PREFIX."End", __FILE__, __LINE__, "pid:" . getmypid() . " " . $log);
                break;
            default:
                break;
        }
    }
}

register_shutdown_function('serverHandleFatal');