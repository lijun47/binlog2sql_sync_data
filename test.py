{
    'template':
    'UPDATE `api_lanlingcb_dev2`.`company_subject` SET `credit_code`=%s, `manage_location`=%s, `legal_person`=%s, `busi_license`=%s, `status`=%s,\
         `reviewer_id`=%s, `reviewer_name`=%s, `create_time`=%s, `update_time`=%s, `remark`=%s, `id_card_front`=%s, `id_card_back`=%s, `bankcard`=%s, \
             `issuing_bank`=%s, `verify_account`=%s, `payment_money`=%s, `is_payment`=%s, `pay_failure_reason`=%s, `bnkflg`=%s, `eaccty`=%s, `bank_outlet`=%s WHERE com_sub_id = 233;',
    'values': [
        '小黄鸭公司2', '52366648899', '河南郑州', '黄宝强',
        'http://pic.pobit.cn/companySubject/20201019/34971250-b0de-419f-ae4b-f2fc1896920a.png',
        '2', None, '', None,
        datetime.datetime(2020, 10, 19, 10, 35,
                          49), '2020-10-19 10:20:18 打款金额核对错误，系统自动驳回',
        'http://pic.pobit.cn/companySubject/20201019/805c07c7-ae97-44ba-bca7-eb554d16da4e.png',
        'http://pic.pobit.cn/companySubject/20201019/f3ebda47-dc1d-4743-bc41-c7516ad5cbf0.png',
        '21355564999994926', '建设银行', 1,
        Decimal('0.07'), 1, None, '', '', None
    ]
}
