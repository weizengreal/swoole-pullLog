<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:43
 */
class LiveTime implements PullerInterface
{

    public function filter($log)
    {
        return true;
    }
}