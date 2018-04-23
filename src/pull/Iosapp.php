<?php

/**
 * Created by PhpStorm.
 * User: weizeng
 * Date: 2017/11/28
 * Time: 下午4:40
 */
class Iosapp implements PullerInterface
{

    public function filter($log)
    {
        $event_fields = [
            // IOS前端采集
            'dts_statistics_appStart',
            'dts_statistics_appEnd',
            'dts_statistics_login',
            'dts_statistics_logout',
            'dts_statistics_pageAppear',
            'dts_statistics_pageDisappear',
            'dts_statistics_clickEvent',
            'dts_statistics_exception',
            'dts_statistics_networkRequest',
            'dts_statistics_choice_data',
            'dts_statistics_choice_data_list',

            // IOS 前端采集 10.20新加
            'dts_statistics_report_view',

            // 双师-作业批改时长
            'dts_statistics_tutor_correct_paper_start',
            'dts_statistics_tutor_correct_paper_end',
            'dts_statistics_tutor_correct_ques_end',
            'dts_statistics_tutor_correct_upload_vedio',

        ];
        if(in_array($log['event_name'],$event_fields)) {
            return true;
        }
        else {
            return false;
        }
    }
}