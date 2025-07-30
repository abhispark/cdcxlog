# ==============================================================================
#                      SELF-SUFFICIENT DATA LOGGER
# ==============================================================================
#
# Author: Your Name
# Version: 1.0
#
# Description:
# A single, standalone Python script to connect to the CoinDCX WebSocket,
# dynamically discover all active INR pairs, and log aggregated candlestick
# data to CSV files. This script is designed to be run 24/7 on a server
# or cloud VM to build a historical dataset for backtesting.
#
# No other files from the trading bot project are required to run this script.
#
# ==============================================================================

# --- 1. Required Libraries ---
# Before running, ensure you have these installed:
# pip install "python-socketio[client]" requests
import socketio
import asyncio
import json
import os
import time
from datetime import datetime, timezone
import logging
import requests

# --- 2. Logger Configuration ---
# All user-configurable settings are in this section.
CONFIG = {
    # WebSocket endpoint for real-time prices
    'ws_endpoint': 'wss://stream.coindcx.com',
    'ws_prices_channel': 'currentPrices@spot@1s',

    # API endpoint for discovering all market details
    'markets_api_url': 'https://api.coindcx.com/exchange/v1/markets_details',
    'quote_asset': 'INR', # The quote asset to filter pairs for (e.g., 'INR', 'USDT')

    # Timeframes for candle aggregation (in seconds).
    # You can log multiple timeframes simultaneously.
    # The '1d' key will be saved into a 'daily' subfolder for the backtester.
    'aggregation_timeframes': {
        '1m': 60,
        '5m': 300,
        '1h': 3600,
        '1d': 86400
    },

    # The base directory where all data will be saved.
    'output_dir': 'historical_data'
}

# --- 3. Setup Logging ---
# Configures a logger to print progress and status to the console.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)-8s - %(message)s'
)
logger = logging.getLogger('DataLogger')


# --- 4. The Data Logger Class ---
# This class encapsulates all the logic for connecting, discovering, and logging.
class DataLogger:
    def __init__(self, config):
        """Initializes the DataLogger instance."""
        self.config = config
        self.sio = socketio.AsyncClient(logger=False, engineio_logger=False)
        self.pairs_to_log = []
        self.pair_symbol_map = {}  # Maps 'BTCINR' -> 'B-BTC_INR'
        self.candles = {}          # In-memory store for active candles

        self._setup_event_handlers()
        self._prepare_output_dirs()

    def _prepare_output_dirs(self):
        """Creates the necessary output directories for each timeframe if they don't exist."""
        base_dir = self.config['output_dir']
        for timeframe_name in self.config['aggregation_timeframes'].keys():
            subfolder = 'daily' if timeframe_name == '1d' else timeframe_name
            dir_path = os.path.join(base_dir, subfolder)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                logger.info(f"Created directory: {dir_path}")

    def discover_pairs(self):
        """
        Dynamically fetches all active pairs from the exchange API based on the
        configured quote asset. This is called once on startup.
        """
        quote_asset = self.config['quote_asset']
        logger.info(f"Discovering all active {quote_asset} markets from the API...")
        try:
            response = requests.get(self.config['markets_api_url'], timeout=10)
            response.raise_for_status()
            all_markets = response.json()

            for market in all_markets:
                if (market.get('status') == 'active' and
                    market.get('base_currency_short_name') == quote_asset):
                    coindcx_name = market.get('coindcx_name')
                    pair_symbol = market.get('pair')
                    if coindcx_name and pair_symbol:
                        self.pairs_to_log.append(coindcx_name)
                        self.pair_symbol_map[coindcx_name] = pair_symbol

            if not self.pairs_to_log:
                logger.error(f"Could not discover any active {quote_asset} pairs. Exiting.")
                return False

            logger.info(f"Successfully discovered {len(self.pairs_to_log)} active {quote_asset} pairs.")
            logger.info(f"Example pairs to be logged: {self.pairs_to_log[:5]}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"FATAL: Could not fetch market details: {e}. Exiting.")
            return False

    def _setup_event_handlers(self):
        """Binds the WebSocket events to class methods."""
        @self.sio.event
        async def connect():
            logger.info("Successfully connected to CoinDCX WebSocket.")
            await self.sio.emit('join', {'channelName': self.config['ws_prices_channel']})
            logger.info(f"Subscribed to price channel: {self.config['ws_prices_channel']}")

        @self.sio.event
        async def disconnect():
            logger.warning("Disconnected from WebSocket.")

        @self.sio.event
        async def connect_error(data):
            logger.error(f"WebSocket connection failed: {data}")

        @self.sio.on(self.config['ws_prices_channel'] + '#update')
        async def on_price_update(response):
            await self._process_price_update(response)

    async def _process_price_update(self, response):
        """The core logic: processes new ticks and aggregates them into candles."""
        try:
            parsed_data = json.loads(response['data'])
            if 'prices' not in parsed_data: return
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Could not parse price update: {response}")
            return

        current_timestamp = time.time()
        for market, price_str in parsed_data['prices'].items():
            if market not in self.pairs_to_log: continue

            try:
                price = float(price_str)
                if price <= 0: continue
            except (ValueError, TypeError): continue

            if market not in self.candles: self.candles[market] = {}

            for tf_name, tf_seconds in self.config['aggregation_timeframes'].items():
                if tf_name not in self.candles[market]:
                    self._create_new_candle(market, tf_name, tf_seconds, price, current_timestamp)
                else:
                    active_candle = self.candles[market][tf_name]
                    if current_timestamp >= active_candle['start_time'] + tf_seconds:
                        self._write_candle_to_csv(market, tf_name, active_candle)
                        self._create_new_candle(market, tf_name, tf_seconds, price, current_timestamp)
                    else:
                        active_candle['high'] = max(active_candle['high'], price)
                        active_candle['low'] = min(active_candle['low'], price)
                        active_candle['close'] = price
                        # Note: The price stream lacks volume data. We use tick count as a proxy.
                        active_candle['volume'] += 1

    def _create_new_candle(self, market, tf_name, tf_seconds, price, timestamp):
        """Initializes a new candle object in the state."""
        start_time = int(timestamp / tf_seconds) * tf_seconds
        self.candles[market][tf_name] = {
            'open': price, 'high': price, 'low': price, 'close': price,
            'volume': 1, 'start_time': start_time
        }
        logger.debug(f"Created new {tf_name} candle for {market} at {datetime.fromtimestamp(start_time, tz=timezone.utc)}")

    def _write_candle_to_csv(self, market, tf_name, candle_data):
        """Appends a completed candle to its corresponding CSV file."""
        subfolder = 'daily' if tf_name == '1d' else tf_name
        pair_symbol = self.pair_symbol_map.get(market)
        if not pair_symbol: return

        file_name = f"{pair_symbol.replace('/', '_')}_{tf_name}.csv"
        file_path = os.path.join(self.config['output_dir'], subfolder, file_name)
        iso_time = datetime.fromtimestamp(candle_data['start_time'], tz=timezone.utc).isoformat().replace('+00:00', 'Z')
        row = f"{iso_time},{candle_data['open']},{candle_data['high']},{candle_data['low']},{candle_data['close']},{candle_data['volume']}\n"
        write_header = not os.path.exists(file_path)

        try:
            with open(file_path, 'a', newline='') as f:
                if write_header:
                    f.write("time,o,h,l,c,v\n")
                f.write(row)
            logger.info(f"Wrote {tf_name} candle for {market} to {file_path}")
        except IOError as e:
            logger.error(f"Failed to write to file {file_path}: {e}")

    async def run(self):
        """Main execution loop."""
        if not self.discover_pairs():
            return

        logger.info("Starting WebSocket connection loop...")
        while True:
            try:
                await self.sio.connect(self.config['ws_endpoint'], transports=['websocket'])
                await self.sio.wait()
            except socketio.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}. Retrying in 15 seconds...")
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}. Retrying...", exc_info=True)
                if self.sio.connected:
                    await self.sio.disconnect()
                await asyncio.sleep(15)


# --- 5. Main Execution Block ---
# This block runs when the script is executed directly.
if __name__ == "__main__":
    logger.info("Initializing Data Logger...")
    data_logger = DataLogger(CONFIG)
    try:
        # Start the asynchronous event loop
        asyncio.run(data_logger.run())
    except KeyboardInterrupt:
        logger.info("\nData logger stopped by user.")
    except Exception as e:
        logger.error(f"A critical error forced the script to exit: {e}", exc_info=True)