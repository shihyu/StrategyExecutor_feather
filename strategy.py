import os
import threading
import time
import logging
import utils
import datetime
import traceback
from abc import ABC, abstractmethod
from zoneinfo import ZoneInfo
from sdk_manager_async import SDKManager, check_sdk
from dotenv import load_dotenv
from fubon_neo.sdk import FubonSDK, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction


class Strategy(ABC):
    """
    Strategy template class
    """

    def __init__(self, logger=None, log_level=logging.DEBUG):
        # Set logger
        if logger is None:
            current_date = datetime.datetime.now(ZoneInfo("Asia/Taipei")).date().strftime("%Y-%m-%d")
            utils.mk_folder("log")
            logger = utils.get_logger(name="Strategy", log_file=f"log/strategy_{current_date}.log",
                                      log_level=log_level)

        self.logger = logger

        # The sdk_manager
        self.sdk_manager = None

        # Coordination
        # self.__is_strategy_run = False

    """
        Public Functions
    """

    def set_sdk_manager(self, sdk_manager: SDKManager):
        self.sdk_manager = sdk_manager
        self.logger.info(f"The SDKManager version: {self.sdk_manager.__version__}")

    @check_sdk
    def add_realtime_marketdata(self, symbol: str):
        """
         Add a realtime trade data websocket channel
        :param symbol: stock symbol (e.g., "2881")
        """
        pass

    @check_sdk
    def remove_realtime_marketdata(self, symbol: str):
        """
        Remove a realtime market data websocket channel
        :param symbol: stock symbol (e.g., "2881")
        """
        pass

    @abstractmethod
    @check_sdk
    def run(self):
        """
        Strategy logic to be implemented.
        """
        raise NotImplementedError("Subclasses must implement this method")


class MyStrategy(Strategy):
    def __init__(self, logger=None, log_level=logging.DEBUG):
        super().__init__(logger=logger, log_level=log_level)

        # Setup target symbols
        self.__symbols = ["3605"]
        # self.__latest_timestamp = {}
        # self.__locks = {}
        # for s in self.__symbols:
        #     self.__locks[s] = threading.Lock()

        # Price data
        self.__lastday_close = {}

        # Order coordinators
        self.__open_order_placed = {}
        self.__position_info = {}
        self.__closure_order_placed = {}

        self.__on_going_orders = {}  # {symbol -> [seq_no]}
        self.__on_going_orders_lock = {}
        for s in self.__symbols:
            self.__on_going_orders[s] = []
            self.__on_going_orders_lock[s] = threading.Lock()

    @check_sdk
    def run(self):
        # Get stock's last day close price
        rest_stock = self.sdk_manager.sdk.marketdata.rest_client.stock

        for s in self.__symbols:
            response = rest_stock.intraday.quote(symbol=s)
            self.__lastday_close[s] = float(response["previousClose"])
            time.sleep(0.1)

        # Set callback functions
        self.sdk_manager.set_trade_handle_func("on_filled", self.__order_filled_processor)
        self.sdk_manager.set_ws_handle_func("message", self.__realtime_price_data_processor)  # Marketdata

        # Subscribe realtime marketdata
        for symbol in self.__symbols:
            self.sdk_manager.subscribe_realtime_trades(symbol)
            time.sleep(0.1)

        # Start position closure and order update agents
        t = threading.Thread(target=self.__order_status_updater)

        t.start()
        self.__position_closure_executor()
        t.join()

    def __order_status_updater(self):
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()
        self.logger.info(f"Start __order_status_updater")

        while now_time < datetime.time(13, 32):
            # Check if anything to check
            if not all(len(lst) == 0 for lst in self.__on_going_orders.values()):
                # Get order results
                try:
                    the_account = self.sdk_manager.active_account
                    response = self.sdk_manager.sdk.get_order_results(the_account)

                    if response.is_success:
                        data = response.data

                        for d in data:
                            try:
                                seq_no = str(d.seq_no)
                                symbol = str(d.stock_no)
                                status = int(d.status)

                                with self.__on_going_orders_lock[symbol]:
                                    if status != 10:
                                        if seq_no in self.__on_going_orders[symbol]:
                                            self.__on_going_orders[symbol].remove(seq_no)

                                            self.logger.debug(
                                                f"on_going_orders updated (order updater): {self.__on_going_orders}"
                                            )
                            except Exception as e:
                                self.logger.debug(f"__order_status_updater error (inner loop) - {e}")
                            finally:
                                continue

                    else:
                        self.logger.debug(f"__order_status_updater retrieve order results failed, " +
                                          f"message {response.message}")

                except Exception as e:
                    self.logger.debug(f"__order_status_updater error - {e}")
                finally:
                    time.sleep(1)
                    continue

            # sleep
            time.sleep(1)

            # Update the time
            now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

    def __position_closure_executor(self):
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

        while now_time < datetime.time(13, 32):
            if now_time > datetime.time(13, 20):
                clean_list = []

                for symbol in self.__position_info.keys():
                    with self.__on_going_orders_lock[symbol]:
                        # DayTrade 全部出場
                        if int(self.__position_info[symbol]["size"]) >= 1000 and \
                                (symbol not in self.__closure_order_placed.keys()) and \
                                (len(self.__on_going_orders[symbol]) == 0):
                            self.logger.info(f"{symbol} 全出場條件成立 ...")

                            qty = self.__position_info[symbol]["size"]

                            order = Order(
                                buy_sell=BSAction.Buy,
                                symbol=symbol,
                                price=None,
                                quantity=int(qty),
                                market_type=MarketType.Common,
                                price_type=PriceType.Market,
                                time_in_force=TimeInForce.ROD,
                                order_type=OrderType.Stock,
                                user_def="hvl_close",
                            )

                            response = self.sdk_manager.sdk.stock.place_order(
                                self.sdk_manager.active_account,
                                order,
                                unblock=False,
                            )

                            if response.is_success:
                                self.logger.info(f"{symbol} 全出場下單成功, size {qty}")
                                self.__closure_order_placed[symbol] = True

                                # Update on_going_orders list
                                if symbol in self.__on_going_orders:
                                    self.__on_going_orders[symbol].append(response.data.seq_no)
                                else:
                                    self.__on_going_orders[symbol] = [response.data.seq_no]

                                self.logger.debug(
                                    f"on_going_orders updated (closure): {self.__on_going_orders}"
                                )
                            else:
                                self.logger.warning(f"{symbol} 全出場下單失敗, size {qty}, msg: {response.message}")

                        else:
                            self.logger.debug(f"全出場條件成立\"未\"成立 ...")
                            self.logger.debug(f"(Closure session) symbol {symbol}")
                            self.logger.debug(f"(Closure session) position info: {self.__position_info}")
                            self.logger.debug(
                                f"(Closure session) closure order placed keys: {self.__closure_order_placed.keys()}")
                            clean_list.append(symbol)

                # Execute position info cleaning
                for symbol in clean_list:
                    try:
                        del self.__position_info[symbol]
                    finally:
                        self.sdk_manager.unsubscribe_realtime_trades(symbol)

            # sleep
            time.sleep(1)

            # Update the time
            now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

    def __realtime_price_data_processor(self, data):
        # self.logger.debug(f"marketdata: {data}")
        try:
            symbol = data["symbol"]
            # timestamp = int(data["time"])
            mid_price = (data["bid"] + data["ask"]) / 2
            is_continuous = True if "isContinuous" in data.keys() else False

            if is_continuous:  #and \
                # (symbol not in self.__latest_timestamp.keys() or timestamp > self.__latest_timestamp[symbol]):

                # with self.__locks[symbol]:
                #     self.__latest_timestamp[symbol] = timestamp

                # Start trading logic =============
                with self.__on_going_orders_lock[symbol]:
                    now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

                    # 開盤動作
                    if now_time < datetime.time(9, 15) and \
                            symbol not in self.__open_order_placed.keys():
                        price_change_pct = \
                            100 * (mid_price - self.__lastday_close[symbol]) / self.__lastday_close[symbol]

                        if 1 < price_change_pct < 6:
                            self.logger.info(f"{symbol} 進場條件成立 ...")

                            order = Order(
                                buy_sell=BSAction.Sell,
                                symbol=symbol,
                                price=None,
                                quantity=1000,
                                market_type=MarketType.Common,
                                price_type=PriceType.Market,
                                time_in_force=TimeInForce.IOC,
                                order_type=OrderType.DayTrade,
                                user_def="hvl_enter",
                            )

                            response = self.sdk_manager.sdk.stock.place_order(
                                self.sdk_manager.active_account,
                                order,
                                unblock=True,
                            )

                            if response.is_success:
                                # Update the order record
                                if symbol in self.__open_order_placed.keys():
                                    self.__open_order_placed[symbol] += 1
                                else:
                                    self.__open_order_placed[symbol] = 1

                                self.logger.info(f"{symbol} 進場下單成功, 進場張數 {self.__open_order_placed[symbol]}")

                                # Update on_going_orders list
                                if symbol in self.__on_going_orders:
                                    self.__on_going_orders[symbol].append(response.data.seq_no)
                                else:
                                    self.__on_going_orders[symbol] = [response.data.seq_no]

                                self.logger.debug(
                                    f"on_going_orders updated (enter): {self.__on_going_orders}"
                                )

                    # 停損停利出場
                    if now_time < datetime.time(13, 20) and \
                            symbol in self.__position_info.keys() and \
                            len(self.__on_going_orders[symbol]) == 0:
                        info = self.__position_info[symbol]
                        sell_price = info["price"]

                        current_pnl_pct = 100 * (sell_price - mid_price) / mid_price

                        if current_pnl_pct >= 6 or current_pnl_pct <= -2:
                            self.logger.info(f"{symbol} 停損/停利條件成立 ...")

                            order = Order(
                                buy_sell=BSAction.Buy,
                                symbol=symbol,
                                price=None,
                                quantity=1000,
                                market_type=MarketType.Common,
                                price_type=PriceType.Market,
                                time_in_force=TimeInForce.IOC,
                                order_type=OrderType.Stock,
                                user_def="hvl_stop",
                            )

                            response = self.sdk_manager.sdk.stock.place_order(
                                self.sdk_manager.active_account,
                                order,
                                unblock=True,
                            )

                            if response.is_success:
                                self.logger.info(f"{symbol} 停損/停利下單成功")

                                # Update on_going_orders list
                                if symbol in self.__on_going_orders:
                                    self.__on_going_orders[symbol].append(response.data.seq_no)
                                else:
                                    self.__on_going_orders[symbol] = [response.data.seq_no]

                                self.logger.debug(
                                    f"on_going_orders updated (stop): {self.__on_going_orders}"
                                )

        except Exception as e:
            self.logger.error(f"__realtime_price_data_processor, error: {e}")
            self.logger.debug(f"\ttraceback:\n{traceback.format_exc()}")

    def __order_filled_processor(self, code, filled_data):
        self.logger.debug(f"__order_filled_processor: code {code}, filled_data\n{filled_data}")

        if filled_data is not None:
            user_def = str(filled_data.user_def)
            seq_no = str(filled_data.seq_no)
            symbol = str(filled_data.stock_no)
            account_no = str(filled_data.account)
            filled_qty = int(filled_data.filled_qty)
            filled_price = float(filled_data.filled_price)

            target_account_no = str(self.sdk_manager.active_account.account)

            if account_no == target_account_no:
                with self.__on_going_orders_lock[symbol]:
                    if user_def == "hvl_enter":
                        if symbol not in self.__position_info:
                            self.__position_info[symbol] = {
                                "price": filled_price,
                                "size": filled_qty
                            }

                        else:
                            original_price = self.__position_info[symbol]["price"]
                            original_size = self.__position_info[symbol]["size"]

                            new_size = original_size + filled_qty
                            new_price = (original_price * original_size + filled_price * filled_qty) / new_size

                            self.__position_info[symbol] = {
                                "price": new_price,
                                "size": new_size
                            }

                        self.logger.debug(f"position_info updated (enter): {self.__position_info}")

                    elif user_def in ["hvl_stop", "hvl_close"]:
                        if symbol not in self.__position_info:
                            self.logger.debug(f"Symbol {symbol} is not in self.__position_info")

                        else:
                            original_size = self.__position_info[symbol]["size"]

                            if filled_qty >= original_size:  # Position closed
                                # Remove position info for the symbol
                                del self.__position_info[symbol]

                                # Unsubscribe realtime market data
                                self.sdk_manager.unsubscribe_realtime_trades(symbol)

                            else:
                                self.__position_info[symbol]["size"] = original_size - filled_qty

                            self.logger.debug(f"position_info updated (stop/closure): {self.__position_info}")

                    # Update on_going_orders
                    if seq_no in self.__on_going_orders[symbol]:
                        self.__on_going_orders[symbol].remove(seq_no)

                    self.logger.debug(
                        f"on_going_orders updated (filled data): {self.__on_going_orders}"
                    )

        else:
            self.logger.error(f"Filled order error event: code {code}, filled_data {filled_data}")


# Main script
if __name__ == '__main__':
    # Load login info as the environment variables
    load_dotenv()  # Load .env
    my_id = os.getenv("ID")
    trade_password = os.getenv("TRADEPASS")
    cert_filepath = os.getenv("CERTFILEPATH")
    cert_password = os.getenv("CERTPASSS")
    active_account = os.getenv("ACTIVEACCOUNT")

    # Create SDKManger instance
    sdk_manager = SDKManager()
    sdk_manager.login(my_id, trade_password, cert_filepath, cert_password)
    sdk_manager.set_active_account_by_account_no(active_account)

    # Strategy
    my_strategy = MyStrategy()
    my_strategy.set_sdk_manager(sdk_manager)
    my_strategy.run()

    # Ending
    print("Ending session ...")
    time.sleep(20)
    sdk_manager.terminate()
    print("program ended, press any key to exit ... ")
    input()
