<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/10
 * Time: 下午2:47
 * Des：这里应该是一个最低级的工作进程
 */

namespace Home\Base;

abstract class ProcessManager
{
    protected $sigterm;

    public function __construct()
    {
        // 存储接收到的所有信号
        $this->sigterm = 0;

    }

    /*
     * 处理接收到的信号量
     * */
    public function killSelf()
    {
        $this->sigterm = time();
    }

    /*
     * 启动一个新的进程
     * */
    protected function newProcess($params = [])
    {
        $process = new \swoole_process(function () use($params){
            $this->action($params);
        });
        return $process->start();
    }


    /*
     * 拉取进程的核心组件及相关工作
     * */
    private function action($param)
    {

        pcntl_signal(SIGTERM, array($this, 'killSelf'));
        declare(ticks=1);
        $pid = getmypid();
        if(PIDMAX != -1 && $pid > PIDMAX) {
            \EagleEye\Classes\Log::error(LOG_PREFIX."Aliyun_Process_Manager",
                'get my pid error!getmypid value is:'.$pid);
            exit();
        }
        $param['myPid'] = $pid;

        $this->startAction($param);
        while ($this->check()) {
            $this->doAction($param);
        }
        $this->stopAction($param);
    }

    /*
     * 处理信号量、设置当前进程处于激活状态
     * */
    protected function check()
    {
        // 检测到 SIGTERM 信号
        if ($this->sigterm != 0) {
            return false;
        }
        return true;
    }


    /*
     * 进程开始的准备工作
     * */
    abstract function startAction($param);


    /*
     * 进程核心逻辑，理论上是一个最小的原子操作
     * */
    abstract function doAction($param);

    /*
     * 进程正常结束之后执行的函数
     * */
    abstract function stopAction($param);

    /*
     * 根据业务逻辑检测自己的进程是否正常
     * */
    abstract function checkSelf();

}