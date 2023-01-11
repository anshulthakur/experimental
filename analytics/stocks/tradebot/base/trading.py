from lib.logging import log
from .graph import BaseClass
import datetime

class Position(BaseClass):
    def __init__(self, buy=None, sell=None, quantity=1):
        if buy is not None and sell is not None:
            log(f'Cannot have both buy and sell in a single order', 'error')
            raise Exception('Cannot have both buy and sell in a single order')
        self.open = True
        self.buy = buy
        self.sell = sell
        self.profit = 0
        self.quantity = quantity
    
    def is_long(self):
        return True if (self.buy is not None and self.sell is None) else False

    def close(self, price):
        if self.is_long():
            self.sell = price
        else:
            self.buy = price
        self.profit = (self.sell - self.buy)*self.quantity
        log(f'Closed position. Profit = {self.profit}', 'info')

class Broker(BaseClass):
    def __init__(self, **kwargs):
        #print(kwargs)
        super().__init__()

    def get_charges(self, buy=True, price=0, quantity=0, segment='equity'):
        if segment=='equity':
            brokerage = min(20, (0.03*price*quantity)/100)
            stt = ((0.025/100)*(price*quantity)) if buy is False else 0
            transaction = (0.00345/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
            #print("Brokerage: {}\nSTT: {}\nTransaction Charges: {}\nGST:{}\nStamp Duty: {}".format(
            #       brokerage,stt,transaction,gst,stamp))
        elif segment=='options':
            brokerage = 20
            stt = ((0.05/100)*(price*quantity)) if buy is False else 0
            transaction = (0.053/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
        elif segment=='commodity':
            quantity = quantity*100 #Commodities have a size of 100
            brokerage = min(20, (0.03*price*quantity)/100) 
            stt = ((0.05/100)*(price*quantity)) if buy is False else 0
            transaction = (0.05/100)*(price*quantity)
            gst = (18/100)*(brokerage+transaction)
            stamp = ((0.003/100)*(price*quantity)) if buy is True else 0
            print(brokerage+stt+transaction+gst+stamp)
        return(brokerage+stt+transaction+gst+stamp)
    
    def get_profit(self, quantity=0, buy=0, sell=0, segment='equity'):
        buy_side = self.get_charges(buy=True, price=buy, quantity=quantity, segment = segment)
        sell_side = self.get_charges(buy=False, price=sell, quantity=quantity, segment = segment)
        gross_profit = (sell-buy)*quantity
        return(gross_profit - buy_side - sell_side)
    
    def get_break_even_profit_margin(self, quantity=0, buy=None, sell=None, segment='equity'):
        #Approximate margin (by doubling the one side brokerage)
        if buy is not None:
            #Long
            margin = 2* self.get_charges(buy=True, price=buy if buy != None else 0, quantity=quantity, segment = segment)
        elif sell is not None:
            #Short
            margin = 2* self.get_charges(buy=False, price=sell if sell!=None else 0, quantity=quantity, segment = segment)
        else:
            print("Neither buy nor sell. Kehna kya chahte ho?")
            return -1
        return margin


class BaseBot(BaseClass):
    def __init__(self, cash=0, lot_size=1, **kwargs):
        #print(kwargs)
        self.orderbook = []
        self.position = None
        self.charges = 0
        self.cash = cash
        self.initial_cash = cash
        self.lot_size = lot_size
        super().__init__()

    def buy(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if self.position.is_long():
                log(f'Already long', 'warning')
                pass
            else:
                log(f'Close shorts', 'info')
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}', 'info')
                self.position = None
        else:
            charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
            if self.cash > ((price*self.lot_size) + charges):
                self.position = Position(buy=price, quantity=self.lot_size)
                self.cash = self.cash - (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                log(f'Not enough cash. Have: {self.cash}. Required: {((price*self.lot_size) + charges)}', 'warning')

    def sell(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if not self.position.is_long():
                log(f'Already short', 'warning')
                pass
            else:
                log(f'Close longs', 'info')
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=False, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
                self.position = None
        else:
            charges = self.get_charges(segment='options', quantity=self.lot_size, buy=False, price=price)
            if self.cash > ((price*self.lot_size) + charges):
                self.position = Position(sell=price, quantity=self.lot_size)
                self.cash = self.cash - (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                log(f'Not enough cash', 'warning')

    def close_position(self, price, date=datetime.datetime.now()):
        if self.position is not None:
            if self.position.is_long():
                self.position.close(price)
                charges = self.get_charges(segment='options', 
                                            quantity=self.lot_size, 
                                            buy=False, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'sell', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
            else:
                self.position.close(price)
                charges = self.get_charges(segment='options', quantity=self.lot_size, buy=True, price=price)
                self.cash = self.cash + (price*self.lot_size) - charges
                self.charges += charges
                self.orderbook.append({'timestamp': date, 
                                       'operation': 'buy', 
                                       'price': price, 
                                       'quantity': self.lot_size, 
                                       'charges': charges})
                #log(f'{self.orderbook[-1]}', 'info')
                log(f'Cash: {self.cash} Charges: {self.charges}')
        self.position = None

    def summary(self):
            profit = ((self.cash - self.initial_cash)/self.initial_cash)*100
            print(f'\tFinal capital:\t{self.cash}\t({profit}%)')
            print(f'\tTrades:\t{len(self.orderbook)}')
            print(f'\tCharges: {self.charges}')

    def get_orderbook(self):
        print(f'Orderbook')
        for order in self.orderbook:
            print(order)
