<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2018/4/23
 * Time: 下午2:52
 */
namespace Home\Utils;

class Helper {

    /**
     * shell命令检测进程pid是否存在
     * @param $pid
     * @return int
     */
    public static function ifrun($pid)
    {
        $cmd = 'ps axu|grep "\b' . $pid . '\b"|grep -v "grep"|wc -l';
        exec("$cmd", $ret);

        $ret = trim($ret[0]);

        if ($ret > 0) {
            return intval($ret);
        } else {
            return 0;
        }
    }

    /**
     * tal自定义服务进程名设置，默认将当前进程名称改为tal_$prefix:typenam如tal_baseserver:master
     * @param $prefix
     * @param $typeName
     */
    public static function setProcessName($prefix, $typeName)
    {
        if (empty($_SERVER['TERM_PROGRAM']) || stripos($_SERVER['TERM_PROGRAM'], 'apple') === false) {
            swoole_set_process_name($prefix . ":" . $typeName);
        }
    }

    /**
     * 加盐数据签名验证合法性
     * @param $param
     * @param $salt
     * @return bool
     */
    public static function checkToken($param, $salt)
    {
        if (!isset($param["token"]) && strlen(trim($param["token"])) == 0) {
            return false;
        }

        $token = $param["token"];
        unset($param["token"]);

        ksort($param);
        $sumstring = http_build_query($param);
        $paramtoken = md5($sumstring . $salt);
        if ($token !== $paramtoken) {
            return false;
        }
        return true;
    }

    /**
     * 简单的封装 json_encode 构建一个 http 返回 msg
     * @param $code
     * @param $msg
     * @param $data
     * @param $version
     * */
    public static function buildResponseResult($code, $msg, $data = array(), $version = "1.0")
    {
        $result = array(
            "code" => $code,
            "msg" => $msg,
            "version" => $version,
            "data" => $data,
            "timestamp" => time(),
        );
        return json_encode($result);
    }
}