import asyncio
import os
import threading
import multiprocessing
import time
import logging
import json
import utils
import datetime
import random
import traceback
from queue import Empty, Full
from typing import Optional
from abc import ABC, abstractmethod
from zoneinfo import ZoneInfo
from sdk_manager_async import SDKManager, check_sdk
from dotenv import load_dotenv
from fubon_neo.sdk import Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction
from concurrent.futures import ThreadPoolExecutor


class Strategy(ABC):
    """
    Strategy template class
    """
    __version__ = "2024.0.6"

    def __init__(self, logger=None, log_level=logging.DEBUG):
        # Set logger
        if logger is None:
            current_date = datetime.datetime.now(ZoneInfo("Asia/Taipei")).date().strftime("%Y-%m-%d")
            utils.mk_folder("log")
            logger = utils.get_logger(name="Strategy", log_file=f"log/strategy_{current_date}.log",
                                      log_level=log_level)

        self.logger = logger

        # The sdk_manager
        self.sdk_manager: Optional[SDKManager] = None

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
        self.sdk_manager.subscribe_realtime_trades(symbol)

    @check_sdk
    def remove_realtime_marketdata(self, symbol: str):
        """
        Remove a realtime market data websocket channel
        :param symbol: stock symbol (e.g., "2881")
        """
        self.sdk_manager.unsubscribe_realtime_trades(symbol)

    @abstractmethod
    @check_sdk
    def run(self):
        """
        Strategy logic to be implemented.
        """
        raise NotImplementedError("Subclasses must implement this method")


class MyStrategy(Strategy):
    __version__ = "2024.7.3"

    def __init__(self, the_queue: multiprocessing.Queue, logger=None, log_level=logging.DEBUG):
        super().__init__(logger=logger, log_level=log_level)

        # Multiprocessing queue
        self.queue: multiprocessing.Queue = the_queue

        # Setup target symbols
        self.__symbols = ['1524', '2033', '6283', '3528', '2008', '1441', '6598', '8942',
                          '8410', '5481', '6265', '4153', '4540', '2423', '4102', '00726B',
                          '3419', '1762', '2038', '1789', '3557', '1732', '6024', '6661',
                          '1439', '4939', '4420', '020023', '8088', '3484', '4111']

        # Price data
        self.__lastday_close = {}
        self.__open_price_today = {}

        # Order coordinators
        self.__position_info = {}
        self.__is_reload = False  # If the program start with reload mode
        utils.mk_folder("position_info_store")
        self.__pos_info_file_path = \
            f"position_info_store/position_info_{datetime.datetime.now(ZoneInfo('Asia/Taipei')).strftime('%Y-%m-%d')}.json"
        # Check if the file exists
        if os.path.exists(self.__pos_info_file_path):
            # Load the dictionary from the JSON file
            with open(self.__pos_info_file_path, 'r') as f:
                self.__position_info = json.load(f)
            self.logger.info(f"Loaded position_info from file:\n {self.__position_info}")
            self.__is_reload = True

        self.__open_order_placed = {}
        self.__closure_order_placed = {}

        self.__on_going_orders = {}  # {symbol -> [order_no]}
        self.__order_type_enter = {}
        self.__order_type_exit = {}
        self.__on_going_orders_lock = {}
        self.__latest_price_timestamp = {}
        self.__max_latest_price_timestamp = 0 if not self.__is_reload else time.time()

        self.__failed_order_count = {}
        self.__failed_exit_order_count = {}
        self.__clean_list = []

        self.__suspend_entering_symbols = []

        for s in self.__symbols:
            self.__on_going_orders[s] = []
            self.__order_type_enter[s] = []
            self.__order_type_exit[s] = []
            self.__on_going_orders_lock[s] = threading.Lock()
            self.__latest_price_timestamp[s] = 0

        # Position sizing
        self.__fund_available = 600000
        self.__enter_lot_limit = 3
        self.__max_lot_per_round = min(2, self.__enter_lot_limit)  # Maximum number of round to send order non-stoping
        self.__fund_available_update_lock = threading.Lock()
        self.__active_target_list = []
        self.logger.info(f"初始可用額度: {self.__fund_available} TWD")
        self.logger.info(f"個股下單上限: {self.__enter_lot_limit} 張")

        # Strategy checkpoint
        minute_digit_offset = [-2, -1, 0, 1, 2]
        second_digit_offset = [*range(-25, 26, 1)]
        self.__strategy_exit_time = datetime.time(13, int(16 + random.choice(minute_digit_offset)),
                                                  int(30 + random.choice(second_digit_offset)))
        self.__strategy_enter_cutoff_time = datetime.time(9, 6)

        # ThreadPoolExecutor
        num_cores = multiprocessing.cpu_count()
        max_workers = max(num_cores * 2, 2)
        self.__threadpool_executor = ThreadPoolExecutor(max_workers=max_workers)
        # self.__logging_executor = ThreadPoolExecutor(max_workers=1)

    def __flush_position_info(self):
        # Send message to the multiprocessing queue
        message = {
            "event": "position_info",
            "data": self.__position_info
        }
        try:
            self.queue.put_nowait(message)
        except Full:
            self.logger.warning("Supervisor's queue is full ...")

    def __emit_heartbeat(self):
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()
        # Update the multiprocessing queue
        message = {
            "event": "strategy_heartbeat",
            "data": now_time
        }
        try:
            self.queue.put_nowait(message)
        except Full:
            self.logger.warning("Supervisor's queue is full ...")

        price_waiting_time = time.time() - self.__max_latest_price_timestamp / 1000000
        if (price_waiting_time > 60) and self.__position_info:
            message = {
                "event": "warning",
                "data": price_waiting_time
            }
            try:
                self.queue.put_nowait(message)
            except Full:
                self.logger.warning("Supervisor's queue is full ...")

    @check_sdk
    def run(self):
        asyncio.run(self.async_run())

        # # Get stock's last day close price
        # rest_stock = self.sdk_manager.sdk.marketdata.rest_client.stock
        #
        # self.logger.debug("Strategy.run - Getting stock close price of the last trading day ...")
        # self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # for s in self.__symbols:
        #     response = rest_stock.intraday.quote(symbol=s)
        #     self.__lastday_close[s] = float(response["previousClose"])
        #     time.sleep(0.1)
        #
        # # Set callback functions
        # self.sdk_manager.set_trade_handle_func("on_filled", self.__order_filled_processor)
        # self.sdk_manager.set_ws_handle_func("message", self.__realtime_price_data_processor)  # Marketdata
        #
        # # Remove symbols that can do day-trade short sell
        # self.logger.debug("Strategy.run - Cleaning symbol list ...")
        # self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # if self.sdk_manager.sdk_version >= (1,3,1):
        #     candidate_symbols = self.__symbols.copy()
        #     self.__symbols = []
        #
        #     for symbol in candidate_symbols:
        #         query = self.sdk_manager.sdk.stock.daytrade_and_stock_info(self.sdk_manager.active_account, symbol)
        #         retry_attempt_count = 0
        #         is_success = False
        #
        #         while not is_success:
        #             if retry_attempt_count >= 2:
        #                 self.logger.info(f"Failed to retrieve daytrade info for {symbol} too many time, skip this target")
        #                 self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #                 break  # Proceed to the next symbol
        #
        #             elif not query.is_success:
        #                 self.logger.debug(f"Fail retrieving daytrade info for {symbol}, retry later ...")
        #                 self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #                 retry_attempt_count += 1
        #
        #             else:
        #                 try:
        #                     if (query.data.status is not None) and (int(query.data.status) >= 8):
        #                         self.logger.debug(f"{symbol}'s status {query.data.status}, add to the list")
        #                         self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #                         self.__symbols.append(symbol)
        #                     else:
        #                         self.logger.info(f"{symbol}'s daytrade status: {query.data.status}, remove from the list")
        #                         self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        #                     is_success = True
        #                     break  # Proceed to the next symbol
        #
        #                 except Exception as e:
        #                     self.logger.error(f"{symbol} - query {query}")
        #                     self.logger.error(f"Daytrade info query exception {e}, traceback {traceback.format_exc()}," +
        #                                       f" retry ...")
        #                     self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #                     retry_attempt_count += 1
        #
        #             # Wait and retry
        #             time.sleep(0.2)
        #             query = self.sdk_manager.sdk.stock.daytrade_and_stock_info(self.sdk_manager.active_account, symbol)
        #
        #     self.logger.debug(f"Original symbol list (length {len(candidate_symbols)}):\n {candidate_symbols}")
        #     self.logger.info(f"Refined symbol list (length {len(self.__symbols)}):\n {self.__symbols}")
        #     self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # else:
        #     self.logger.debug(f"SDK version is less than 1.3.1, current {self.sdk_manager.sdk_version}, ignore symbol cleaning ...")
        #     self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # self.logger.debug("Strategy.run - Subscribing realtime market datafeed ...")
        # self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # # Subscribe realtime marketdata
        # for symbol in self.__symbols:
        #     self.sdk_manager.subscribe_realtime_trades(symbol)
        #     time.sleep(0.1)
        #
        # self.logger.debug("Strategy.run - Market data preparation has completed.")
        # self.__emit_heartbeat()  # Send heartbeat to the supervisor
        #
        # # Order update agents
        # t = threading.Thread(target=self.__order_status_updater)
        #
        # # Position sizing agent
        # tps = threading.Thread(target=self.__position_sizing_agent)
        #
        # tps.start()
        # t.start()
        # self.__position_closure_executor()  # Position closure agent
        # tps.join()
        # t.join()

    async def async_run(self):
        # Get stock's last day close price
        rest_stock = self.sdk_manager.sdk.marketdata.rest_client.stock

        self.logger.debug("Strategy.run - Getting stock close price of the last trading day ...")
        self.__emit_heartbeat()  # Send heartbeat to the supervisor

        for s in self.__symbols:
            response = rest_stock.intraday.quote(symbol=s)
            self.__lastday_close[s] = float(response["previousClose"])
            await asyncio.sleep(0.1)

        # Set callback functions
        self.sdk_manager.set_trade_handle_func("on_filled", self.__order_filled_processor)
        self.sdk_manager.set_ws_handle_func("message", self.__realtime_price_data_processor)  # Marketdata

        # Remove symbols that can do day-trade short sell
        self.logger.debug("Strategy.run - Cleaning symbol list ...")
        self.__emit_heartbeat()  # Send heartbeat to the supervisor

        if self.sdk_manager.sdk_version >= (1, 3, 1):
            candidate_symbols = self.__symbols.copy()
            self.__symbols = []

            for symbol in candidate_symbols:
                self.__emit_heartbeat()  # Send heartbeat to the supervisor
                query = self.sdk_manager.sdk.stock.daytrade_and_stock_info(self.sdk_manager.active_account, symbol)
                retry_attempt_count = 0
                is_success = False

                while not is_success:
                    if retry_attempt_count >= 2:
                        self.logger.info(
                            f"Failed to retrieve daytrade info for {symbol} too many time, skip this target")
                        break  # Proceed to the next symbol

                    elif not query.is_success:
                        self.logger.debug(f"Fail retrieving daytrade info for {symbol}, retry later ...")
                        retry_attempt_count += 1

                    else:
                        try:
                            if (query.data.status is not None) and (int(query.data.status) >= 8):
                                self.logger.debug(f"{symbol}'s status {query.data.status}, add to the list")
                                self.__symbols.append(symbol)
                            else:
                                self.logger.info(
                                    f"{symbol}'s daytrade status: {query.data.status}, remove from the list")

                            is_success = True
                            break  # Proceed to the next symbol

                        except Exception as e:
                            self.logger.error(f"{symbol} - query {query}")
                            self.logger.error(
                                f"Daytrade info query exception {e}, traceback {traceback.format_exc()}," +
                                f" retry ...")
                            self.__emit_heartbeat()  # Send heartbeat to the supervisor
                            retry_attempt_count += 1

                    # Wait and retry
                    await asyncio.sleep(0.2)
                    query = self.sdk_manager.sdk.stock.daytrade_and_stock_info(self.sdk_manager.active_account, symbol)

            self.logger.debug(f"Original symbol list (length {len(candidate_symbols)}):\n {candidate_symbols}")
            self.logger.info(f"Refined symbol list (length {len(self.__symbols)}):\n {self.__symbols}")
            self.__emit_heartbeat()  # Send heartbeat to the supervisor

        else:
            self.logger.debug(
                f"SDK version is less than 1.3.1, current {self.sdk_manager.sdk_version}, ignore symbol cleaning ...")
            self.__emit_heartbeat()  # Send heartbeat to the supervisor

        self.logger.debug("Strategy.run - Subscribing realtime market datafeed ...")
        self.__emit_heartbeat()  # Send heartbeat to the supervisor

        # Subscribe realtime marketdata
        for symbol in self.__symbols:
            self.sdk_manager.subscribe_realtime_trades(symbol)
            await asyncio.sleep(0.1)

        self.logger.debug("Strategy.run - Market data preparation has completed.")
        self.__emit_heartbeat()  # Send heartbeat to the supervisor

        # Order update agents
        t = asyncio.create_task(self.__order_status_updater())

        # Position sizing agent
        tps = asyncio.create_task(self.__position_sizing_agent())

        await self.__position_closure_executor()  # Position closure agent
        await tps
        await t

    async def __position_sizing_agent(self):
        """
            Add 6 more symbols every 1 second after market open
        """
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

        while now_time < datetime.time(8, 59, 58):
            await asyncio.sleep(1)
            now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

        self.logger.debug(f"開始啟動加入進場標的 (time {now_time}) ...")

        while now_time < datetime.time(9, 5, 15):
            if len(self.__active_target_list) == len(self.__symbols):
                break

            i = 0
            for symbol in self.__symbols:
                if i > 6:
                    break

                if symbol not in self.__active_target_list:
                    self.__active_target_list.append(symbol)
                    i += 1

            # self.logger.debug(f"現有進場標的列表 (time {now_time}): {self.__active_target_list}")

            await asyncio.sleep(1)

            now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

        # All symbol included
        self.__active_target_list = self.__symbols
        self.logger.debug(f"全進場標的列表 (time {now_time}): {self.__active_target_list}")

    async def __order_status_updater(self):
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()
        self.logger.info(f"Start __order_status_updater")

        while now_time < datetime.time(13, 32):
            # Send heartbeat to the supervisor
            self.__emit_heartbeat()

            # Check if anything to check
            if not all(len(lst) == 0 for lst in self.__on_going_orders.values()):
                # Get order results
                try:
                    the_account = self.sdk_manager.active_account

                    response = await asyncio.get_event_loop().run_in_executor(
                        self.__threadpool_executor,
                        self.sdk_manager.sdk.stock.get_order_results,
                        the_account
                    )
                    # response = self.sdk_manager.sdk.stock.get_order_results(the_account)

                    if response.is_success:
                        data = response.data

                        for d in data:
                            try:
                                order_no = str(d.order_no)
                                symbol = str(d.stock_no)
                                status = int(d.status)

                                with self.__on_going_orders_lock[symbol]:
                                    if status != 10:
                                        if order_no in self.__on_going_orders[symbol]:
                                            self.__on_going_orders[symbol].remove(order_no)

                                            self.logger.debug(
                                                f"on_going_orders updated (order updater): symbol {symbol}, " +
                                                f"order_no {order_no}"  # {self.__on_going_orders}"
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

            # sleep
            await asyncio.sleep(1)

            # Update the time
            now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

    def __position_closure_executor_symbol(self, symbol):
        with self.__on_going_orders_lock[symbol]:
            # DayTrade 全部出場
            if int(self.__position_info[symbol]["size"]) >= 1000 and \
                    (symbol not in self.__closure_order_placed) and \
                    (len(self.__on_going_orders[symbol]) == 0):
                self.logger.info(f"{symbol} 時間出場條件成立 ...")

                qty = int(self.__position_info[symbol]["size"])

                self.logger.debug(f"{symbol} 出場股數 {qty}")

                while qty >= 1000:
                    order = Order(
                        buy_sell=BSAction.Buy,
                        symbol=symbol,
                        price=None,
                        quantity=1000,
                        market_type=MarketType.Common,
                        price_type=PriceType.Market,
                        time_in_force=TimeInForce.ROD,
                        order_type=OrderType.Stock,
                        user_def="hvl_close",
                    )

                    place_order_start_time = time.time()

                    response = self.sdk_manager.sdk.stock.place_order(
                        self.sdk_manager.active_account,
                        order,
                        unblock=False,
                    )

                    place_order_end_time = time.time()

                    if response.is_success:
                        self.logger.info(f"{symbol} 時間出場下單成功, size {qty}")
                        self.logger.debug(f"速度 {1000*(place_order_end_time - place_order_start_time):.6f} ms, " +
                                          f"非由行情觸發. Data:\n{response.data}")

                        self.__closure_order_placed[symbol] = True

                        # Update order_type_exit
                        if symbol in self.__order_type_exit:
                            self.__order_type_exit[symbol].append(response.data.order_no)
                        else:
                            self.__order_type_exit[symbol] = [response.data.order_no]

                        # Update on_going_orders list
                        if symbol in self.__on_going_orders:
                            self.__on_going_orders[symbol].append(response.data.order_no)
                        else:
                            self.__on_going_orders[symbol] = [response.data.order_no]

                        self.logger.debug(
                            f"on_going_orders updated (closure): symbol {symbol}, " +
                            f"order_no {response.data.order_no}"  # {self.__on_going_orders}"
                        )

                        # Update qty
                        qty -= 1000
                        self.logger.debug(f"{symbol} 剩餘出場股數 {qty}")

                        # Pause
                        sec_to_sleep = random.randint(5, 10)
                        time.sleep(sec_to_sleep)

                    else:
                        self.logger.warning(
                            f"{symbol} 時間出場下單失敗, size {qty}, msg: {response.message}")
                        break

            else:
                self.logger.debug(f"時間出場條件\"未\"成立 ...")
                self.logger.debug(f"(Closure session) symbol {symbol}")
                self.logger.debug(f"(Closure session) position info: {self.__position_info}")
                self.logger.debug(
                    f"(Closure session) closure order placed keys: {self.__closure_order_placed.keys()}")
                self.__clean_list.append(symbol)

    async def __position_closure_executor(self):
        now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

        while now_time < datetime.time(13, 32):
            try:
                if now_time > self.__strategy_exit_time:
                    self.__clean_list = []

                    symbols_to_check = set(self.__position_info.keys()).copy()  # Symbols for this round of iteration

                    loop = asyncio.get_event_loop()

                    futures = [
                        loop.run_in_executor(self.__threadpool_executor, self.__position_closure_executor_symbol, symbol)
                        for symbol in symbols_to_check
                    ]

                    # Await all futures
                    await asyncio.gather(*futures)

                    # Execute position info cleaning
                    for symbol in self.__clean_list:
                        try:
                            del self.__position_info[symbol]
                        finally:
                            self.sdk_manager.unsubscribe_realtime_trades(symbol)

                    # self.__flush_position_info() is not necessary here
                    self.__clean_list = []

            except Exception as e:
                self.logger.debug(f"時間出場迴圈錯誤: {e}, traceback: {traceback.format_exc()}")

            finally:
                # sleep
                await asyncio.sleep(1)

                # Update the time
                now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

    def __realtime_price_data_processor(self, data):
        try:
            symbol = data["symbol"]
            is_continuous = True if "isContinuous" in data else False
            is_open = True if "isOpen" in data else False

            if is_open:
                self.logger.info(f"Market open {symbol}: {data}")

            # Cutout stock that does not fit the open price goal
            if (symbol not in self.__open_price_today) and \
                    (is_open or is_continuous) and \
                    ("price" in data) and (not self.__is_reload):

                self.__open_price_today[symbol] = float(data["price"])

                gap_change_pct = 100 * (self.__open_price_today[symbol] - self.__lastday_close[symbol]) \
                                 / self.__lastday_close[symbol]

                if (gap_change_pct < 1) or (gap_change_pct > 6):  # Do not trade this target today
                    self.logger.info(f"{symbol} 開盤漲幅超過區間 (實際漲幅: {gap_change_pct:.2f} %)，移除標的")
                    self.__open_order_placed[symbol] = 99999
                    self.sdk_manager.unsubscribe_realtime_trades(symbol)
                    return

            # Start trading logic =============
            if is_continuous or is_open:
                with self.__on_going_orders_lock[symbol]:
                    timestamp = int(data["time"])
                    if timestamp > self.__latest_price_timestamp.get(symbol, timestamp):
                        self.__latest_price_timestamp[symbol] = timestamp

                        # Update the newest timestamp over all symbols
                        if timestamp > self.__max_latest_price_timestamp:
                            self.__max_latest_price_timestamp = timestamp
                    else:
                        return

                    # Start processing this tick
                    now_time = datetime.datetime.now(ZoneInfo("Asia/Taipei")).time()

                    # 開盤動作
                    if (now_time < self.__strategy_enter_cutoff_time) and \
                            (not self.__is_reload) and \
                            (symbol not in self.__suspend_entering_symbols) and \
                            (symbol not in self.__open_order_placed or
                             self.__open_order_placed[symbol] < self.__enter_lot_limit):

                        if symbol not in self.__active_target_list:
                            self.logger.info(f"{symbol} 尚未納入進場列表: {self.__active_target_list}")

                        elif ("bid" not in data) or (float(data["bid"]) == 0):
                            self.logger.debug(f"{symbol} 進場判斷邏輯讀無 bid 價格, data:\n{data}")

                        else:
                            # Calculate price change
                            try:
                                baseline_price = data["price"] if "price" in data else data["bid"]
                            except Exception as e:
                                baseline_price = data["bid"]

                                self.logger.debug(
                                    f"{symbol} read price exception {e}. data: {data}. " +
                                    f"Traceback: {traceback.format_exc()}"
                                )

                            price_change_pct_bid = 100 * (float(baseline_price) - self.__lastday_close[symbol]) / \
                                                   self.__lastday_close[symbol]

                            # Check for entering condition
                            quantity_to_bid = 0
                            pre_allocate_fund = 0

                            if 1 < price_change_pct_bid < 5:
                                with self.__fund_available_update_lock:
                                    max_share_possible = \
                                        int((self.__fund_available / float(data["bid"])) / 1000) * 1000

                                    # Calculate quantity to bid
                                    if max_share_possible < 1000:
                                        self.logger.info(f"剩餘可用額度不足, symbol {symbol}, price {data['bid']}, " +
                                                         f"fund_available {self.__fund_available}")
                                        self.logger.info(f"{symbol} 稍後再確認進場訊號 ...")

                                    else:
                                        quantity_to_bid = int(self.__enter_lot_limit * 1000)
                                        if symbol in self.__open_order_placed:
                                            quantity_to_bid -= self.__open_order_placed[symbol] * 1000

                                        self.logger.debug(f"{symbol} 剩餘下單量 {quantity_to_bid} 股")

                                        if max_share_possible < quantity_to_bid:
                                            self.logger.debug(f"剩餘額動不足進場 {symbol}: {quantity_to_bid} 股," +
                                                              f" 調整下單量至 1 張")
                                            quantity_to_bid = 1000

                                        # Pre-allocate fund
                                        scale_factor = min(quantity_to_bid, self.__max_lot_per_round * 1000)
                                        pre_allocate_fund = scale_factor * float(baseline_price)
                                        self.__fund_available -= pre_allocate_fund

                                        self.logger.info(f"{symbol} 預留下單額度更新: {pre_allocate_fund}")
                                        self.logger.info(f"可用額度更新: {self.__fund_available}")

                            # 進場操作
                            if quantity_to_bid >= 1000:
                                self.logger.info(f"{symbol} 進場條件成立 ...")

                                quantity_has_bid = 0  # Record how much has already been bid this time

                                while quantity_to_bid >= 1000:
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

                                    place_order_start_time = time.time()

                                    response = self.sdk_manager.sdk.stock.place_order(
                                        self.sdk_manager.active_account,
                                        order,
                                        unblock=False,
                                    )

                                    place_order_end_time = time.time()

                                    if response.is_success:
                                        self.logger.debug(f"{symbol} 下單成功! " + f"成功進場委託單直回:\n{response}")
                                        self.logger.debug(
                                            f"下單前置 {1000*(place_order_start_time - data['ws_received_time']):.6f} ms, " +
                                            f"速度 {1000*(place_order_end_time - place_order_start_time):.6f} ms, " +
                                            f"觸發行情資料:\n {data}"
                                        )

                                        # Update the order record
                                        if symbol in self.__open_order_placed:
                                            self.__open_order_placed[symbol] += 1
                                        else:
                                            self.__open_order_placed[symbol] = 1

                                        self.logger.info(
                                            f"{symbol} 進場下單成功, 總進場張數 {self.__open_order_placed[symbol]}")

                                        # Update pre-allocated fund
                                        pre_allocate_fund -= 1000 * float(baseline_price)
                                        self.logger.info(f"{symbol} 預留下單額度更新: {pre_allocate_fund}")

                                        # Update the order_type_enter list
                                        if symbol in self.__order_type_enter:
                                            self.__order_type_enter[symbol].append(response.data.order_no)
                                        else:
                                            self.__order_type_enter[symbol] = [response.data.order_no]

                                        # Update on_going_orders list
                                        if symbol in self.__on_going_orders:
                                            self.__on_going_orders[symbol].append(response.data.order_no)
                                        else:
                                            self.__on_going_orders[symbol] = [response.data.order_no]

                                        self.logger.debug(
                                            f"on_going_orders updated (enter): symbol {symbol}, " +
                                            f"order_no {response.data.order_no}"
                                        )

                                        # Update remain quantity_to_bid
                                        quantity_to_bid -= 1000
                                        quantity_has_bid += 1000

                                        self.logger.debug(f"{symbol} 已下單 {quantity_has_bid} 股，" +
                                                          f"剩餘下單量 {quantity_to_bid} 股")

                                        # Bid max shares per round
                                        if quantity_has_bid >= self.__max_lot_per_round * 1000:
                                            break

                                    else:
                                        self.logger.debug(f"失敗進場委託單直回:\n{response}")

                                        if symbol in self.__failed_order_count:
                                            self.__failed_order_count[symbol] += 1
                                        else:
                                            self.__failed_order_count[symbol] = 1

                                        if self.__failed_order_count[symbol] >= 2:
                                            if (symbol in self.__open_order_placed) and \
                                                    (self.__open_order_placed[symbol] > 0):
                                                self.logger.info(f"{symbol} 已進場 " +
                                                                 f"{self.__open_order_placed[symbol]} 張, " +
                                                                 f'剩餘量委託失敗次數過多取消')
                                                self.logger.info(f"{symbol} 不再確認進場訊號 ...")
                                                self.__suspend_entering_symbols.append(symbol)

                                        else:
                                            self.logger.info(f"{symbol} 進場下單失敗次數達 2 次且未進場，移除標的")
                                            self.__open_order_placed[symbol] = 99999
                                            self.sdk_manager.unsubscribe_realtime_trades(symbol)

                                        # Cancel ramin quantity_to_bid
                                        quantity_to_bid = -99999

                                # Adjust for actual used fund
                                with self.__fund_available_update_lock:
                                    self.__fund_available += pre_allocate_fund
                                    self.logger.info(f"{symbol} 實際運用差額: {pre_allocate_fund}")
                                    self.logger.info(f"可用額度更新: {self.__fund_available}")

                    elif (symbol not in self.__open_order_placed) and (not self.__is_reload):  # 今天完全沒進場
                        self.logger.info(f"{symbol} 今日無進場，移除股價行情訂閱 ...")
                        self.sdk_manager.unsubscribe_realtime_trades(symbol)

                    # 停損停利出場
                    if now_time < self.__strategy_exit_time and \
                            symbol in self.__position_info and \
                            len(self.__on_going_orders[symbol]) == 0:
                        info = self.__position_info[symbol]
                        sell_price = info["price"]

                        ask_price = float(data["ask"]) if ("ask" in data and float(data["ask"]) > 0) else float(
                            data["price"])  # Add if for robustness
                        gap_until_now_pct = 100 * (ask_price - self.__lastday_close[symbol]) / self.__lastday_close[
                            symbol]
                        current_pnl_pct = 100 * (sell_price - ask_price) / ask_price

                        is_early_session = now_time <= datetime.time(9, 30)
                        stop_condition_zeta = (gap_until_now_pct >= 8)
                        stop_condition_1 = is_early_session and \
                                           ((current_pnl_pct >= 8) or (current_pnl_pct <= -5))
                        stop_condition_2 = (not is_early_session) and \
                                           ((current_pnl_pct >= 6) or (current_pnl_pct <= -3))

                        if stop_condition_zeta or stop_condition_1 or stop_condition_2:
                            self.logger.info(f"{symbol} 停損/停利條件成立 ...")

                            if stop_condition_zeta:
                                self.logger.info(f"{symbol} 漲幅達 {gap_until_now_pct} %, 斷然出場！")
                                order = Order(
                                    buy_sell=BSAction.Buy,
                                    symbol=symbol,
                                    price=None,
                                    quantity=int(self.__position_info[symbol]["size"]),
                                    market_type=MarketType.Common,
                                    price_type=PriceType.Market,
                                    time_in_force=TimeInForce.ROD,
                                    order_type=OrderType.Stock,
                                    user_def="hvl_stop",
                                )
                            else:
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

                            # 下單
                            if symbol not in self.__failed_exit_order_count or \
                                    self.__failed_exit_order_count[symbol] < 5:

                                place_order_start_time = time.time()

                                response = self.sdk_manager.sdk.stock.place_order(
                                    self.sdk_manager.active_account,
                                    order,
                                    unblock=False,
                                )

                                place_order_end_time = time.time()

                                if response.is_success:
                                    self.logger.info(f"{symbol} 停損/停利下單成功")
                                    self.logger.debug(
                                        f"下單前置 {1000 * (place_order_start_time - data['ws_received_time']):.6f} ms, " +
                                        f"速度 {1000 * (place_order_end_time - place_order_start_time):.6f} ms, " +
                                        f"觸發行情資料:\n {data}"
                                    )

                                    # Update the order_type_exit list
                                    if symbol in self.__order_type_exit:
                                        self.__order_type_exit[symbol].append(response.data.order_no)
                                    else:
                                        self.__order_type_exit[symbol] = [response.data.order_no]

                                    # Update on_going_orders list
                                    if symbol in self.__on_going_orders:
                                        self.__on_going_orders[symbol].append(response.data.order_no)
                                    else:
                                        self.__on_going_orders[symbol] = [response.data.order_no]

                                    self.logger.debug(
                                        f"on_going_orders updated (stop): symbol {symbol}, " +
                                        f"order_no {response.data.order_no}"  # {self.__on_going_orders}"
                                    )
                                else:
                                    self.logger.debug(f"失敗停損出場委託單直回:\n{response}")

                                    if symbol in self.__failed_exit_order_count:
                                        self.__failed_exit_order_count[symbol] += 1
                                    else:
                                        self.__failed_exit_order_count[symbol] = 1

                                    if self.__failed_exit_order_count[symbol] >= 5:
                                        self.logger.warning(f"{symbol} 停損/利下單失敗次數達 5 次!")
                            else:
                                self.logger.warning(f"{symbol} 停損/利下單失敗次數過多! 手動處理!")
                                self.sdk_manager.unsubscribe_realtime_trades(symbol)

        except Exception as e:
            self.logger.error(f"__realtime_price_data_processor, error: {e}")
            self.logger.debug(f"\ttraceback:\n{traceback.format_exc()}")
            self.logger.debug(f"\treceived data: {data}")

    def __order_filled_processor(self, code, filled_data):
        self.logger.debug(f"__order_filled_processor: code {code}, filled_data\n{filled_data}")

        if filled_data is not None:
            try:
                order_no = str(filled_data.order_no)
                symbol = str(filled_data.stock_no)
                account_no = str(filled_data.account)
                filled_qty = int(filled_data.filled_qty)
                filled_price = float(filled_data.filled_price)

                target_account_no = str(self.sdk_manager.active_account.account)

                if account_no == target_account_no and (symbol in self.__on_going_orders_lock):
                    with self.__on_going_orders_lock[symbol]:
                        if order_no in self.__order_type_enter[symbol]:
                            if symbol not in self.__position_info:
                                self.__position_info[symbol] = {
                                    "price": filled_price,
                                    "size": filled_qty
                                }
                                self.__flush_position_info()

                            else:
                                original_price = self.__position_info[symbol]["price"]
                                original_size = self.__position_info[symbol]["size"]

                                new_size = original_size + filled_qty
                                new_price = (original_price * original_size + filled_price * filled_qty) / new_size

                                self.__position_info[symbol] = {
                                    "price": new_price,
                                    "size": new_size
                                }
                                self.__flush_position_info()

                            self.logger.debug(f"position_info updated (enter): {self.__position_info}")

                        elif order_no in self.__order_type_exit[symbol]:
                            if symbol not in self.__position_info:
                                self.logger.debug(f"Symbol {symbol} is not in self.__position_info")

                            else:
                                original_size = self.__position_info[symbol]["size"]

                                if filled_qty >= original_size:  # Position closed
                                    # Remove position info for the symbol
                                    del self.__position_info[symbol]
                                    self.__flush_position_info()

                                    # Unsubscribe realtime market data
                                    self.sdk_manager.unsubscribe_realtime_trades(symbol)

                                else:
                                    self.__position_info[symbol]["size"] = original_size - filled_qty
                                    self.__flush_position_info()

                                self.logger.debug(f"position_info updated (stop/closure): {self.__position_info}")

                        else:
                            self.logger.debug(f"Unregistered order {order_no}, ignore.")
                                              # f"\tenter orders - {self.__order_type_enter}\n" +
                                              # f"\texit orders - {self.__order_type_exit}")

                            return

                        # Update on_going_orders
                        if order_no in self.__on_going_orders[symbol]:
                            self.__on_going_orders[symbol].remove(order_no)

                        self.logger.debug(
                            f"on_going_orders updated (filled data): symbol {symbol}, " +
                            f"order_no {order_no}"  # {self.__on_going_orders}"
                        )
                else:
                    self.logger.debug(f"Unregistered order, ignore. account - {account_no}")
                                      # f"\tenter orders - {self.__order_type_enter}\n" +
                                      # f"\texit orders - {self.__order_type_exit}")

            except Exception as e:
                self.logger.error(f"Filled data processing error - {e}. Filled data:\n{filled_data}")

        else:
            self.logger.error(f"Filled order error event: code {code}, filled_data {filled_data}")


def main(convoy_queue: multiprocessing.Queue):
    try:
        # Send the starting check message to the multiprocessing queue
        message = {
            "event": "start",
            "data": None
        }
        convoy_queue.put(message)

        # Load login info as the environment variables
        load_dotenv()  # Load .env
        my_id = os.getenv("ID")
        trade_password = os.getenv("TRADEPASS")
        cert_filepath = os.getenv("CERTFILEPATH")
        cert_password = os.getenv("CERTPASSS")
        active_account = os.getenv("ACTIVEACCOUNT")

        # Create SDKManger instance
        sdk_manager = SDKManager()
        # print("Strategy: logging in ...")
        sdk_manager.login(my_id, trade_password, cert_filepath, cert_password)
        # print("Strategy: Set account No. ...")
        sdk_manager.set_active_account_by_account_no(active_account)

        # Strategy
        my_strategy = MyStrategy(convoy_queue)
        # print("Strategy: Assume SDK manager ...")
        my_strategy.set_sdk_manager(sdk_manager)
        # print("Strategy: Start running ...")
        my_strategy.run()

        # Ending
        print("Ending session ...")
        sdk_manager.terminate()
        time.sleep(5)

        # Send the terminating message to the multiprocessing queue
        message = {
            "event": "terminate",
            "data": None
        }
        convoy_queue.put(message)

    except Exception as e:
        print(f"Main script exception {e}, traceback:\n{traceback.format_exc()}")

    finally:
        print("program ended")


# Main script
if __name__ == '__main__':
    utils.mk_folder("log")
    utils.mk_folder("position_info_store")
    current_date = datetime.datetime.now(ZoneInfo("Asia/Taipei")).date().strftime("%Y-%m-%d")
    logger = utils.get_logger(name="Strategy_supervisor", log_file=f"log/strategy_supervisor_{current_date}.log",
                              log_level=logging.DEBUG)

    # Start the multiprocessing procedure
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=main, args=(queue,))
    p.start()

    while True:
        try:
            # Try to get a message from the queue with a timeout
            message = queue.get(timeout=30)

            if message["event"] == "strategy_heartbeat":
                pass

            elif message["event"] == "position_info":
                pos_info_file_path = \
                    f"position_info_store/position_info_{datetime.datetime.now(ZoneInfo('Asia/Taipei')).strftime('%Y-%m-%d')}.json"

                # Save the position info
                try:
                    # Load the dictionary from a JSON file
                    with open(pos_info_file_path, 'w') as f:
                        json.dump(message["data"], f)

                    logger.debug(f"position_info updated: {message['data']}")

                except Exception as e:
                    logger.debug(f"Flush position_info to file failed!!! Exception: {e}, " +
                                 f"traceback:\n{traceback.format_exc()}")

            elif message["event"] == "terminate":
                logger.info("Supervisor exiting ...")
                break

            elif message["event"] == "warning":
                logger.warning(f"Over {message['data']} seconds, there is no price comes in!")

            else:
                logger.debug(f"Queue message: {message}")

        except Empty:
            # If the queue is empty, terminate the process
            logger.debug("Operation timed out. Terminating process and starting a new one.")
            p.terminate()
            p.join(timeout=10)

            # Start a new process
            p = multiprocessing.Process(target=main, args=(queue,))
            p.start()

    # Exiting
    os._exit(0)
