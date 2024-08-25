import os
import logging
import datetime
import asyncio
from multiprocessing import Process, Queue, Event
from logging.handlers import QueueHandler, QueueListener


# Create logger
def get_logger(name=None, log_file='program.log', log_level=logging.INFO):
    shutdown_event = Event()  # Create a shutdown event

    # Create a logger
    log_name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") if name is None else name
    new_logger = logging.getLogger(log_name)
    new_logger.setLevel(log_level)

    # Create a formatter
    formatter = logging.Formatter('[%(asctime)s:%(levelname)s] %(name)s: %(message)s')

    # Setup multiprocessing queue and log handlers
    log_queue_one = Queue()
    log_queue_two = Queue()

    queue_handler_channel_one = QueueHandler(log_queue_one)
    queue_handler_channel_one.setFormatter(formatter)
    #new_logger.addHandler(queue_handler_channel_one)

    queue_handler_channel_two = QueueHandler(log_queue_two)
    queue_handler_channel_two.setFormatter(formatter)
    new_logger.addHandler(queue_handler_channel_two)

    # Start another process for logging
    Process(
        target=log_queue_listener_starter,
        args=(log_queue_one,
              log_queue_two,
              name,
              log_file,
              log_level,
              shutdown_event)
    ).start()

    return new_logger, shutdown_event

def log_queue_listener_starter(log_queue_one, log_queue_two, name, log_file, log_level, shutdown_event):
    async def log_process_keep_running():
        while not shutdown_event.is_set():
            await asyncio.sleep(30)

    # Create a logger
    log_name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") if name is None else name
    new_logger = logging.getLogger(log_name)
    new_logger.setLevel(log_level)

    # Create a file handler
    #file_handler = logging.FileHandler(log_file)
    #file_handler.setLevel(log_level)

    # Create a stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)

    # Create a formatter
    formatter = logging.Formatter('%(message)s')

    # Set the formatter for both handlers
    #file_handler.setFormatter(formatter)
    #queue_listener_file = QueueListener(log_queue_one, file_handler)

    stream_handler.setFormatter(formatter)
    queue_listener_stream = QueueListener(log_queue_two, stream_handler)

    #queue_listener_file.start()
    queue_listener_stream.start()

    # Keep running
    asyncio.run(log_process_keep_running())
    #queue_listener_file.stop()
    queue_listener_stream.stop()

# Timestamp to datetime
def timestamp_to_datetime(timestamp, tz=None):
    if int(timestamp) >= 10000000000:
        ts = int(timestamp) / 1000  # ms to s
    else:
        ts = int(timestamp)

    result_datetime = datetime.datetime.fromtimestamp(ts)

    if tz is not None:
        result_datetime = tz.localize(result_datetime)

    return result_datetime


# Check and make folder
def mk_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")
    else:
        print(f"{folder_path} folder already exists.")