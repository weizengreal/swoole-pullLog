<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:39
 */
class Interaction implements PullerInterface
{

    function filter($log)
    {
//        if($log['ali_time'] > 1511654400) {
//            return false;
//        }
        $event_fields = [
            // 课中主讲
            'IPS_EXAM_FRONT_OPEN',
            'IPS_EXAM_FRONT_CLOSE',
            'IPS_EXAM_AFTER_OPEN',
            'IPS_EXAM_AFTER_CLOSE',
            'EXAMPLE_OPEN',
            'EXAMPLE_CLOSE',
            'BREAK_TIME_OPEN',
            'BREAK_TIME_CLOSE',
            'REDPOINT_LUCK_OPEN',
            'REDPOINT_AVG_OPEN',
            'REDPOINT_CLOSE',
            'EVALUATION_OPEN',
            'EVALUATION_CLOSE',
            'EVALUATION_REDPOINT_OPEN',
            'EVALUATION_REDPOINT_CLOSE',
            'INTERACTION_CHOICE_OPEN',
            'INTERACTION_CHOICE_CLOSE',
            'INTERACTION_NON_OPEN',
            'INTERACTION_NON_CLOSE',
            'EXAMPLE_SUBJECTIVE_OPEN',
            'EXAMPLE_SUBJECTIVE_CLOSE',

            // 1020
            'INTERACTION_ATTENTION_OPEN',
            'INTERACTION_ATTENTION_CLOSE',

            //1120
            'INTERACTION_UNDERSTAND_OPEN',
            'INTERACTION_UNDERSTAND_CLOSE',

            // 1215
            'INTERACTION_RUBRIC_OPNE',
            'INTERACTION_RUBRIC_CLOSE',

            // 0120
            'INTERACTION_CALL_NAME_OPEN',
            'INTERACTION_CALL_NAME_CLOSE',

            // 0314
            'EVALUATION_PARENT_OPEN',
            'EVALUATION_PARENT_CLOSE',
            'LCT_CURTAIN_OPEN',
            'LCT_CURTAIN_CLOSE',

            // 课中辅导
            'SEND_POINT_MANY',
            'SUBJECTIVE_RECORD',

            'CLASS_RED_MULT',
            'INTERACTION_ATTENTION_CLASS_CLOSE',

            // 0314
            'CLASS_PANEL_SCALE',

            // 课中学生
            'STU_PRETEST',
            'STU_AFTERTEST',
            'STU_HOMEWORK_STAR',
            'STU_RED_POINT',
            'STU_EVALUATE_TCH',
            'STU_EVALUATE_RED_POINT',
            'STU_INTERACTION_CHOICE',
            'STU_INTERACTION_JUDGMENT',
            'STU_SEND_POINT_MANY',
            'STU_RIGHT_RANK',

            'STU_ATTENTION_START',
            'STU_MVP',

            // 1215
            'STU_PRACTICE_STAR',

            // 小游戏的开始和结束
            'PLAY_TIME_OPEN',
            'PLAY_TIME_CLOSE',
            'PLAY_TIME_TUG',

            // 主辅积分数据采集  9/20
            'TCH_POINT_ISSUE',
            'TCH_POINT_REMAIN',

            // 0314
            'STU_CHECK_ATTEND',
            'STU_PART_RANK',
        ];
        if(in_array($log['event_name'],$event_fields)) {
            return true;
        }
        else {
            return false;
        }
    }
}