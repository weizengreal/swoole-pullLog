<?php
/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/24
 * Time: 下午4:45
 */


class Playtime implements PullerInterface {


    public function filter($log)
    {
//        if($log['ali_time'] > 1511654400) {
//            return false;
//        }
        if(!isset($log['answer_tool']) && !isset($log['event_name'])) {
            return false;
        }
        $event_fields = [
            'STU_PLAY_SIGN',
            'STU_PLAY_CLICK',
        ];

        if(isset($log['event_name']) && in_array($log['event_name'],$event_fields)) {
            return true;
        }

        if($log["answer_tool"] == "student_list"
           || $log["answer_tool"] == "bind"
           || $log["answer_tool"] == "unbind"
           || $log["answer_tool"] == "sign_up"
           || $log["answer_tool"] == "click_count_statistic"
        ) {
            return true;
        }

        return false;
    }
}