<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/30
 * Time: 上午11:59
 */

namespace Home\Base;

interface ConsumerInterface
{
    public function batchConsume($logs,$params);
}