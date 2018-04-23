<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:41
 */
class Facemirror implements PullerInterface
{

    public function filter($log)
    {
        $event_fields = [
            'LESSON_FACE_SUM',
        ];
        if(in_array($log['event_name'],$event_fields)) {
            return true;
        }
        else {
            return false;
        }
    }
}