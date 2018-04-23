<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2018/4/23
 * Time: 下午2:27
 */

include "./vendor/autoload.php";

include "./vendor/aliyun_log_sdk/Log_Autoload.php";

spl_autoload_register("load");

function load($className) {
    if(strpos($className,"Home\\") === 0) {
        $filePath = str_replace(["Home","\\"],[ROOT_PATH."src","/"],$className);
        require_once $filePath.".php";
    }
}
