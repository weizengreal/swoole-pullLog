<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:42
 */
class LiveEvent implements PullerInterface
{
    public function filter($log)
    {
        $filterArr = [
            'StartPush',
            'StopPush',
            'StartPlayUrl',
            'StopPlayUrl',
            'OpenCameraError',
            'OpenMicError',
            'NetworkError',
            'NetworkResume',
            'BufferNull',
            'BufferPlay',
            'PushUrlError',
            'PlayUrlError',
        ];
        if(in_array($log['Action'],$filterArr)) {
            return true;
        }
        return false;
    }
}