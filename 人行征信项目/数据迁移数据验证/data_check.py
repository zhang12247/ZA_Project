import mysql.connector
from mysql.connector import errorcode
import unittest
from datetime import datetime


class MyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            MyTest.cnx = mysql.connector.connect(
                host="rm-bp1utr02m6tp303p9.mysql.rds.aliyuncs.com",
                port=3306,
                user="za_dev_frhuld",
                password="za_dev_frhuld_0ab0bf",
                database="fcp_renhang_credit_upload_00",
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        MyTest.cur = MyTest.cnx.cursor()

    @classmethod
    def tearDownClass(cls) -> None:
        MyTest.cur.close()
        MyTest.cnx.close()

    # 如果repayment存在追偿为N不上报，一定存在该期正常还款(NYDD产品除外)
    @unittest.skip("已执行")
    def test_opb_opic_opvo_nor(self):
        resdict = []
        query = "SELECT loan_contract_no,repayment_period,product_code FROM old_pboc_repayment_detail where repayment_type = '3' and flag = 'N'"
        MyTest.cur.execute(query)
        for per_data in MyTest.cur:
            resdict.append(per_data)
        for i in resdict:
            loan_contract_no, period, product_code = i
            query = "select count(1) from old_pboc_repayment_detail where repayment_type = '1' and flag = 'Y' and loan_contract_no = '{}' and repayment_period ='{}'".format(
                loan_contract_no, period
            )
            MyTest.cur.execute(query)
            for per_data in MyTest.cur:
                count, *_ = per_data
                msg = "查询结果不匹配：{}-----{}".format(loan_contract_no, period)
                if product_code == "NYDDXJD":
                    self.assertEqual(0, count, msg)
                else:
                    self.assertEqual(1, count, msg)

    # 宽限期内的追偿，不上报flag为N，宽限期外的追偿上报flag为Y，并且有预上报日期
    @unittest.skip("已执行")
    def test_old_repaydetail_zc(self):
        resdict = []
        query = "SELECT loan_contract_no,repayment_period,pre_report_date,flag FROM old_pboc_repayment_detail where repayment_type = '2'"
        MyTest.cur.execute(query)
        for per_data in MyTest.cur:
            resdict.append(per_data)
        for i in resdict:
            loan_contract_no, period, report_date, dc_flag = i
            query = "select repayment_date,flag,pre_report_date from old_pboc_repayment_detail where repayment_type = '3' and loan_contract_no = '{}' and repayment_period ='{}'".format(
                loan_contract_no, period
            )
            MyTest.cur.execute(query)
            for per_data in MyTest.cur:
                repayment_date, zc_flag, pre_report_date, *_ = per_data
                dc_report_date = datetime.strptime(report_date, "%Y%m%d")
                zc_repayment_date = datetime.strptime(repayment_date, "%Y%m%d")
                interval_day = dc_report_date - zc_repayment_date
                if interval_day.days >= 0:
                    self.assertEqual(
                        "N",
                        zc_flag,
                        "追偿上报状态有误：{}---{}".format(loan_contract_no, period),
                    )
                    self.assertEqual(
                        "N",
                        dc_flag,
                        "代偿上报状态有误：{}---{}".format(loan_contract_no, period),
                    )
                else:
                    self.assertEqual(
                        "Y",
                        zc_flag,
                        "追偿上报状态有误：{}---{}".format(loan_contract_no, period),
                    )
                    self.assertEqual(
                        "Y",
                        dc_flag,
                        "代偿上报状态有误：{}---{}".format(loan_contract_no, period),
                    )
                    self.assertIsNotNone(
                        pre_report_date,
                        "预上报日期内容有误：{}---{}".format(loan_contract_no, period),
                    )
            query = "select count(1) from old_pboc_repayment_detail where repayment_type = '3' and loan_contract_no = '{}' and repayment_period ='{}'".format(
                loan_contract_no, period
            )
            MyTest.cur.execute(query)
            for count_date in MyTest.cur:
                count, *_ = count_date
                if count == 0:
                    self.assertEqual(
                        "Y",
                        dc_flag,
                        "代偿上报状态有误：{}---{}".format(loan_contract_no, period),
                    )

    # repayment_detail表期数存在提前结清，则对应代偿责任段必定是解除状态
    @unittest.skip("已执行")
    def test_old_dc_liabilityState(self):
        resdict = []
        query = "SELECT loan_contract_no,repayment_period FROM old_pboc_repayment_detail where adv_settle = 1"
        MyTest.cur.execute(query)
        for per_data in MyTest.cur:
            resdict.append(per_data)
        for i in resdict:
            loan_contract_no, period, *_ = i
            query = "select effective_state from old_pboc_vicarious_liability where loan_contract_no = '{}' and period ='{}'".format(
                loan_contract_no, period
            )
            MyTest.cur.execute(query)
            for per_data in MyTest.cur:
                effective_state, *_ = per_data
                self.assertEqual(
                    "2",
                    effective_state,
                    "代偿责任解除状态有误：{}---{}".format(loan_contract_no, period),
                )

    # 代偿责任段解除，必定代偿余额为0
    # @unittest.skip("已执行")
    def test_old_dc_liability_balance(self):
        query = "SELECT loan_contract_no,insurance_balance FROM old_pboc_vicarious_liability where effective_state = 2"
        MyTest.cur.execute(query)
        for per_data in MyTest.cur:
            loan_contract_no, insurance_balance, *_ = per_data
            self.assertEqual(
                    0,
                    int(float(insurance_balance)),
                    "在保余额金额有误：{}".format(loan_contract_no),
                )


if __name__ == "__main__":
    unittest.main()