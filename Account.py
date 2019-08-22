import datetime
import pytz


class Account:
    """ Simple account class with balance"""

    @staticmethod
    def _current_time(self):
        utc = datetime.datetime.now()
        return pytz.utc.localize(utc)

    def __init__(self, name, balance):
        self._name = name
        self.__balance = balance
        self._transaction_list = [(Account._current_time(self), balance)]
        print("Account created for ", self._name)

    def deposit(self, amount):
        if amount > 0:
            self.__balance += amount
            self.show_balance()
            self._transaction_list.append((Account._current_time(self), amount))
            self.show_transaction()

    def withdraw(self, amount):
        if 0 < amount <= self.__balance:
            self.__balance -= amount
            self._transaction_list.append((Account._current_time(self), -amount))
        else:
            print("The amount must be greater than zero and no more then your account balance")
        self.show_balance()

    def show_balance(self):
        print("Balance is {}".format(self.__balance))

    def show_transaction(self):
        for date, amount in self._transaction_list:
            if amount > 0:
                tran_type = "deposit"
            else:
                tran_type = "withdraw"
                amount *= -1
            print("{:6} {} on {} (local time was {})".format(amount, tran_type, date, datetime.datetime.utcnow()))


if __name__ == '__main__':
    lar = Account("Lar", 0)
    lar.show_balance()
    lar.show_transaction()

    lar.deposit(1000)
    lar.deposit(25000)
    lar.deposit(500)

    steph = Account("Steph", 800)
    steph.deposit(100)
    steph.withdraw(200)
    steph.show_transaction()
    steph.show_balance()
    print(steph.__dict__)
    steph._Account__balance = 40
    steph.show_balance()

#######################output#########################
# Account created for  Lar
#     Balance is 0
# Balance is 1000
# 1000 deposit on 2019-05-25 22:59:51.889734+00:00 (local time was 2019-05-25 19:59:51.889734)
# Balance is 26000
# 1000 deposit on 2019-05-25 22:59:51.889734+00:00 (local time was 2019-05-25 19:59:51.892600)
# 25000 deposit on 2019-05-25 22:59:51.892600+00:00 (local time was 2019-05-25 19:59:51.892600)
# Balance is 26500
# 1000 deposit on 2019-05-25 22:59:51.889734+00:00 (local time was 2019-05-25 19:59:51.892600)
# 25000 deposit on 2019-05-25 22:59:51.892600+00:00 (local time was 2019-05-25 19:59:51.892750)
# 500 deposit on 2019-05-25 22:59:51.892600+00:00 (local time was 2019-05-25 19:59:51.892750)
#
#print(steph.__dict__)#
# Balance is 700
# {'_name': 'Steph', '_Account__balance': 700, '_transaction_list': [(datetime.datetime(2019, 5, 25, 23, 13, 27, 503054, tzinfo=<UTC>), 800), (datetime.datetime(2019, 5, 25, 23, 13, 27, 503054, tzinfo=<UTC>), 100), (datetime.datetime(2019, 5, 25, 23, 13, 27, 503054, tzinfo=<UTC>), -200)]}
