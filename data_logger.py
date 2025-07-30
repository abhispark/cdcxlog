# websocket_data_logger.py
import socketio
import asyncio
import json
import os
import time
from datetime import datetime, timezone
import logging
import requests # Added for fetching market details

# --- Setup basic logging for the logger process ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('DataLogger')

# ==============================================================================
# 1. LOGGER CONFIGURATION
# ==============================================================================
CONFIG = {
    # --- WebSocket Connection ---
    'ws_endpoint': 'wss://stream.coindcx.com',
    'ws_prices_channel': 'currentPrices@spot@1s',

    # --- API for Pair Discovery ---
    'markets_api_url': 'https://api.coindcx.com/exchange/v1/markets_details',
    'quote_asset': 'INR',

    # --- Timeframes for Candle Aggregation (in seconds) ---
    'aggregation_timeframes': {
        # Uncomment other timeframes if you need them
        '1m': 60,
        '5m': 300,
        '1h': 3600,
        '1d': 86400
    },

    # --- Output Directory ---
    'output_dir': 'historical_data'
}

# ==============================================================================
# 2. THE DATA LOGGER CLASS
# ==============================================================================
class DataLogger:
    def __init__(self, config):
        self.config = config
        self.sio = socketio.AsyncClient(logger=False, engineio_logger=False)
        
        # Will be populated by discover_inr_pairs()
        self.pairs_to_log = []
        self.pair_symbol_map = {} # Maps 'BTCINR' -> 'B-BTC_INR'
        
        # In-memory store for the current, in-progress candle for each pair and timeframe
        self.candles = {}
        
        self._setup_event_handlers()
        self._prepare_output_dirs()

    def _prepare_output_dirs(self):
        """Creates the necessary output directories for each timeframe."""
        base_dir = self.config['output_dir']
        for timeframe_name in self.config['aggregation_timeframes'].keys():
            subfolder = 'daily' if timeframe_name == '1d' else timeframe_name
            dir_path = os.path.join(base_dir, subfolder)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                logger.info(f"Created directory: {dir_path}")

    def discover_inr_pairs(self):
        """
        Dynamically fetches all active INR pairs from the exchange API.
        This method is called once on startup.
        """
        logger.info("Discovering all active INR markets from the API...")
        try:
            response = requests.get(self.config['markets_api_url'], timeout=10)
            response.raise_for_status()
            all_markets = response.json()
            
            # Reset lists before populating
            self.pairs_to_log = []
            self.pair_symbol_map = {}

            for market in all_markets:
                if (market.get('status') == 'active' and 
                    market.get('base_currency_short_name') == self.config['quote_asset']):
                    
                    coindcx_name = market.get('coindcx_name')
                    pair_symbol = market.get('pair')
                    
                    if coindcx_name and pair_symbol:
                        self.pairs_to_log.append(coindcx_name)
                        self.pair_symbol_map[coindcx_name] = pair_symbol
            
            if not self.pairs_to_log:
                logger.error("Could not discover any active INR pairs. Check API or config. Exiting.")
                return False

            logger.info(f"Successfully discovered {len(self.pairs_to_log)} active INR pairs to log.")
            # Optional: Log the first few pairs for confirmation
            logger.info(f"Example pairs: {self.pairs_to_log[:5]}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"FATAL: Could not fetch market details from API: {e}. Exiting.")
            return False
        except Exception as e:
            logger.error(f"FATAL: An unexpected error occurred during pair discovery: {e}. Exiting.")
            return False

    def _setup_event_handlers(self):
        """Binds the WebSocket events to class methods."""
        self.sio.on('connect', self.on_connect)
        self.sio.on('disconnect', self.on_disconnect)
        self.sio.on('connect_error', self.on_connect_error)
        self.sio.on('currentPrices@spot#update', self.on_price_update)

    async def on_connect(self):
        logger.info("Successfully connected to CoinDCX WebSocket.")
        await self.sio.emit('join', {'channelName': self.config['ws_prices_channel']})
        logger.info(f"Subscribed to price channel: {self.config['ws_prices_channel']}")

    async def on_disconnect(self):
        logger.warning("Disconnected from WebSocket.")

    async def on_connect_error(self, data):
        logger.error(f"WebSocket connection failed: {data}")

    async def on_price_update(self, response):
        """The core logic: processes new ticks and aggregates them into candles."""
        try:
            parsed_data = json.loads(response['data'])
            if 'prices' not in parsed_data:
                return
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Could not parse price update: {response}")
            return

        current_timestamp = time.time()
        
        for market, price_str in parsed_data['prices'].items():
            if market not in self.pairs_to_log:
                continue

            try:
                price = float(price_str)
                if price <= 0: continue
            except (ValueError, TypeError):
                continue
            
            if market not in self.candles:
                self.candles[market] = {}

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
        
        # Use the map created during discovery to get the correct symbol for the filename
        pair_symbol = self.pair_symbol_map.get(market)
        if not pair_symbol:
            logger.warning(f"Could not find pair symbol for {market}, skipping write.")
            return

        file_name = f"{pair_symbol}_{tf_name}.csv"
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
        """Main loop to connect and run the logger indefinitely."""
        # --- Run pair discovery before starting the WebSocket loop ---
        if not self.discover_inr_pairs():
            return # Exit if discovery fails

        logger.info("Starting WebSocket Data Logger...")
        while True:
            try:
                await self.sio.connect(self.config['ws_endpoint'], transports=['websocket'])
                await self.sio.wait()
            except socketio.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}. Retrying in 15 seconds...")
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}. Retrying in 15 seconds...")
                if self.sio.connected:
                    await self.sio.disconnect()
                await asyncio.sleep(15)

# ==============================================================================
# 3. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # Ensure the required libraries are installed
    try:
        import socketio
        import requests
    except ImportError:
        print("Required libraries not found. Please run:")
        print("pip install python-socketio[client] requests")
    else:
        data_logger = DataLogger(CONFIG)
        try:
            asyncio.run(data_logger.run())
        except KeyboardInterrupt:
            logger.info("Data logger stopped by user.")
