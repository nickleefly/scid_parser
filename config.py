"""
Configuration module for SCID to PostgreSQL import.

Loads and manages configuration from config.json.
"""

import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str

    def get_connection_string(self) -> str:
        """Get asyncpg connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class ContractConfig:
    """Configuration for a single contract file."""
    file: str
    start_date: Optional[str]
    end_date: Optional[str]


@dataclass
class SymbolConfig:
    """Configuration for a symbol (ES, NQ, etc.)."""
    table_name: str
    price_multiplier: float
    contracts: List[ContractConfig]


class Config:
    """
    Configuration manager for SCID import.

    Loads configuration from config.json and provides typed access.
    """

    DEFAULT_CONFIG = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres",
            "database": "future_index"
        },
        "symbols": {
            "ES": {
                "table_name": "ES",
                "price_multiplier": 1.0,
                "contracts": []
            },
            "NQ": {
                "table_name": "NQ",
                "price_multiplier": 1.0,
                "contracts": []
            }
        }
    }

    def __init__(self, config_path: str = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to config.json. If None, uses default in same directory.
        """
        if config_path is None:
            config_path = Path(__file__).parent / "config.json"

        self.config_path = Path(config_path)
        self._config: Dict = {}
        self.load()

    def load(self) -> None:
        """Load configuration from file."""
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
        else:
            print(f"Config file not found at {self.config_path}, using defaults.")
            self._config = self.DEFAULT_CONFIG.copy()

    def save(self) -> None:
        """Save current configuration to file."""
        with open(self.config_path, 'w') as f:
            json.dump(self._config, f, indent=4)

    def create_default_config(self) -> None:
        """Create a default config.json file."""
        self._config = self.DEFAULT_CONFIG.copy()
        self.save()
        print(f"Created default config at {self.config_path}")

    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration."""
        db = self._config.get("database", self.DEFAULT_CONFIG["database"])
        return DatabaseConfig(
            host=db.get("host", "localhost"),
            port=db.get("port", 5432),
            user=db.get("user", "postgres"),
            password=db.get("password", "postgres"),
            database=db.get("database", "future_index")
        )

    def get_symbol_config(self, symbol: str) -> Optional[SymbolConfig]:
        """
        Get configuration for a symbol.

        Args:
            symbol: Symbol name (ES, NQ, etc.)

        Returns:
            SymbolConfig or None if not found
        """
        symbols = self._config.get("symbols", {})
        sym_config = symbols.get(symbol)

        if not sym_config:
            return None

        contracts = []
        for c in sym_config.get("contracts", []):
            contracts.append(ContractConfig(
                file=c.get("file", ""),
                start_date=c.get("start_date"),
                end_date=c.get("end_date")
            ))

        return SymbolConfig(
            table_name=sym_config.get("table_name", symbol),
            price_multiplier=sym_config.get("price_multiplier", 1.0),
            contracts=contracts
        )

    def get_all_symbols(self) -> List[str]:
        """Get list of all configured symbols."""
        return list(self._config.get("symbols", {}).keys())

    def add_contract(
        self,
        symbol: str,
        file_path: str,
        start_date: str,
        end_date: Optional[str] = None
    ) -> None:
        """
        Add a contract to a symbol's configuration.

        Args:
            symbol: Symbol name (ES, NQ, etc.)
            file_path: Path to SCID file
            start_date: Start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
        """
        if "symbols" not in self._config:
            self._config["symbols"] = {}

        if symbol not in self._config["symbols"]:
            self._config["symbols"][symbol] = {
                "table_name": symbol,
                "price_multiplier": 1.0,
                "contracts": []
            }

        self._config["symbols"][symbol]["contracts"].append({
            "file": file_path,
            "start_date": start_date,
            "end_date": end_date
        })


# --- Usage Example ---
if __name__ == "__main__":
    config = Config()

    # Create default config if it doesn't exist
    if not config.config_path.exists():
        config.create_default_config()

    # Example: Add contracts
    # config.add_contract("ES", r"C:\SierraChart\Data\ESZ24_FUT_CME.scid", "2024-09-15", "2024-12-20")
    # config.add_contract("ES", r"C:\SierraChart\Data\ESH25_FUT_CME.scid", "2024-12-20", None)
    # config.save()

    print(f"Database: {config.database}")
    print(f"Symbols: {config.get_all_symbols()}")

    for sym in config.get_all_symbols():
        sym_config = config.get_symbol_config(sym)
        if sym_config:
            print(f"\n{sym}: {len(sym_config.contracts)} contracts")
            for c in sym_config.contracts:
                print(f"  - {c.file}: {c.start_date} to {c.end_date}")
