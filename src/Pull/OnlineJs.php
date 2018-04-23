<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:41
 */
namespace Home\Pull;

class OnlineJs implements \Home\Base\PullerInterface
{

    public function filter($log)
    {
        return true;
    }
}