"""
Redis Pub for Tbot on Tradingboat
"""
import os
import sys
import json
from dataclasses import dataclass

from distutils.util import strtobool
from redis import Redis, ConnectionPool
from loguru import logger
from components.actions.base.action import Action


# Change the log levelfor loguru
logger.remove()
logger.add(sys.stderr, level=os.environ.get("TBOT_LOGLEVEL", "INFO"))


# This is a prefix for the Redis pub/sub channel name used to communicate between TVWB and TBOT.
REDIS_CHANNEL = "REDIS_CH_"
# This is a prefix for the Redis stream key used to communicate between TVWB and TBOT.
REDIS_STREAM_KEY = "REDIS_SKEY_"
# This is a key used in the Redis stream dictionary to identify the data from TradingBoat.
REDIS_STREAM_TB_KEY = "tradingboat"

TBOT_CLIENT_MAX_LEN = 4


@dataclass
class RedisClient:
    """Redis Client for TradingView's ClientId"""
    stream_key: str
    channel: str
    pool: ConnectionPool = None
    connection: Redis = None


class RedisPubActionClients(Action):
    """Class for handling Redis connections for message delivery.

    This class sets up a Redis connection either as a stream or as a
    pub/sub channel, based on the value of the environment variable
    `TBOT_USES_REDIS_STREAM`.
    """

    def __init__(self):
        super().__init__()
        self.clients = []
        self.is_redis_stream = strtobool(
            os.getenv("TBOT_USES_REDIS_STREAM", "1"))
        self.create_connection_pools_for_clients()

    def create_connection_pools_for_clients(self, nums: int = TBOT_CLIENT_MAX_LEN):
        """Create Connection Pools for Each TV Client"""
        for idx in range(nums + 1):
            client = RedisClient(stream_key=REDIS_STREAM_KEY + str(idx),
                                 channel=REDIS_CHANNEL + str(idx))
            self.clients.append(client)
            if idx > 0:
                self.connect_redis_host(client)
        logger.success(f'Created {TBOT_CLIENT_MAX_LEN} clients')

    def validate_broker_data(self):
        """Validate Message"""
        try:
            data = self.validate_data()
            return data
        except ValueError:
            return None

    def connect_redis_host(self, client: RedisClient):
        """Connect to Redis via either unix or tcp """
        password = os.getenv("TBOT_REDIS_PASSWORD", "")
        host = os.getenv("TBOT_REDIS_HOST", "127.0.0.1")
        unix_sock = os.getenv('TBOT_REDIS_UNIXDOMAIN_SOCK', '')
        unix = {
            'password': password,
            'decode_responses': True,
            "max_connections": 10,
        }
        tcp = {
            'host': host,
            'port': int(os.getenv("TBOT_REDIS_PORT", "6379")),
            'password': password,
            'decode_responses': True,
            "max_connections": 10,
            }
        try:
            if host:
                client.pool = ConnectionPool(**tcp)
                client.connection = Redis(connection_pool=client.pool)
            else:
                redis_url = f"unix://{unix_sock}"
                client.pool = ConnectionPool.from_url(redis_url, **unix)
                client.connection = Redis(**unix)
            if self.is_redis_stream:
                logger.success(f"Connected to Redis| {client.stream_key}:"
                               f"{REDIS_STREAM_TB_KEY}")
            else:
                logger.success(f"Connected to Redis| {client.channel}")
        except ConnectionRefusedError as err:
            logger.error(err)

    def run_redis_stream(self):
        """Add data to the stream"""
        data_dict = self.validate_broker_data()
        if data_dict:
            client_id = int(data_dict.get("clientId", -1))
            if client_id <= 0 or client_id > TBOT_CLIENT_MAX_LEN:
                logger.critical(f'Invalid clientId={client_id} from TradingView')
                return
            # Create a bespoken dictionary for Redis Stream
            stream_dict = {REDIS_STREAM_TB_KEY: json.dumps(data_dict)}
            client = self.clients[client_id]
            client.connection.xadd(
                client.stream_key, stream_dict)
            logger.success(
                f"->pushed|{client.stream_key}:{REDIS_STREAM_TB_KEY}"
            )

    def run_redis_pubsub(self):
        """Publish message"""
        data_dict = self.validate_broker_data()
        # Publishing data
        if data_dict:
            client_id = data_dict.get("clientId", -1)
            if client_id <= 0 or client_id > TBOT_CLIENT_MAX_LEN:
                logger.critical(f'Invalid clientId={client_id}')
                return
            json_string = json.dumps(data_dict)
            client = self.clients[client_id]
            client.connection.publish(client.channel, json_string)
            logger.success(
                f"->pushed| {client.channel}"
            )

    def run(self, *args, **kwargs):
        """
        Custom run method. Add your custom logic here.
        """
        super().run(*args, **kwargs)  # this is required
        if self.is_redis_stream:
            self.run_redis_stream()
        else:
            self.run_redis_pubsub()
