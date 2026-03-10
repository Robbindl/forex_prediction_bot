"""
🛡️ RISK MANAGER - Basic risk management for trading
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional
import threading
from logger import logger


class RiskManager:
    """
    🛡️ Basic Risk Management System
    - Position sizing
    - Daily loss limits
    - Drawdown protection
    """
    
    def __init__(self, account_balance: float = 10000, config_file: str = "risk_config.json"):
        self.account_balance = account_balance
        self.initial_balance = account_balance
        self.config_file = config_file
        
        # Risk parameters
        self.config = self._load_config()
        
        # Trading state
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.total_pnl = 0.0
        self.peak_balance = account_balance
        self.current_drawdown = 0.0
        self.is_killed = False
        self.kill_reason = ""
        self.last_reset_day = datetime.now().date()
        
        # Max positions
        self.max_positions = self.config.get('max_open_positions', 5)
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        logger.info("Risk Manager Initialized")
        logger.info(f"Max Risk Per Trade: {self.config.get('max_risk_per_trade', 2.0)}%")
        logger.info(f"Max Daily Loss: {self.config.get('max_daily_loss_percent', 5.0)}%")
        logger.info(f"Max Drawdown: {self.config.get('max_drawdown_percent', 15.0)}%")
        logger.info(f"Max Positions: {self.max_positions}")
    
    def _load_config(self) -> Dict:
        """Load risk configuration from file or use defaults"""
        defaults = {
            'max_risk_per_trade': 2.0,
            'max_daily_loss_percent': 5.0,
            'max_drawdown_percent': 15.0,
            'max_daily_trades': 10,
            'min_confidence_threshold': 0.65,
            'risk_reward_min': 1.5,
            'max_open_positions': 5
        }
        
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    loaded = json.load(f)
                    defaults.update(loaded)
                    logger.info(f"Loaded risk config from {self.config_file}")
        except Exception as e:
            logger.warning(f"Using default risk config: {e}")
        
        return defaults
    
    def calculate_position_size(self, entry_price: float, stop_loss: float, 
                               confidence: float = 1.0) -> Dict:
        """
        Calculate position size based on risk parameters
        
        Returns:
            Dict with position sizing details
        """
        with self.lock:
            # Check if trading is killed
            if self.is_killed:
                logger.warning(f"Position size request rejected: Trading killed - {self.kill_reason}")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Trading killed: {self.kill_reason}"
                }
            
            # Check daily loss limit
            daily_loss_percent = (self.daily_pnl / self.initial_balance) * 100
            max_daily_loss = self.config.get('max_daily_loss_percent', 5.0)
            
            if daily_loss_percent <= -max_daily_loss:
                logger.warning(f"Position rejected: Daily loss limit reached ({daily_loss_percent:.1f}%)")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Daily loss limit reached ({daily_loss_percent:.1f}%)"
                }
            
            # Check drawdown
            max_dd = self.config.get('max_drawdown_percent', 15.0)
            if self.current_drawdown >= max_dd:
                logger.warning(f"Position rejected: Max drawdown reached ({self.current_drawdown:.1f}%)")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Max drawdown reached ({self.current_drawdown:.1f}%)"
                }
            
            # Check daily trade count
            max_trades = self.config.get('max_daily_trades', 10)
            if self.daily_trades >= max_trades:
                logger.warning(f"Position rejected: Max daily trades reached ({max_trades})")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Max daily trades reached ({max_trades})"
                }
            
            # Check confidence threshold
            min_confidence = self.config.get('min_confidence_threshold', 0.65)
            if confidence < min_confidence:
                logger.warning(f"Position rejected: Confidence too low ({confidence:.2f} < {min_confidence})")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Confidence too low ({confidence:.2f} < {min_confidence})"
                }
            
            # Calculate risk amount
            max_risk = self.config.get('max_risk_per_trade', 2.0)
            risk_percent = max_risk * confidence  # Scale risk by confidence
            risk_amount = self.account_balance * (risk_percent / 100)
            
            # Calculate position size based on stop loss distance
            if entry_price == stop_loss:
                logger.warning("Position rejected: Entry price equals stop loss")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': "Entry price equals stop loss"
                }
            
            price_risk = abs(entry_price - stop_loss)
            position_size = risk_amount / price_risk
            
            # Check risk/reward ratio (simple calculation)
            if entry_price > stop_loss:  # LONG
                potential_profit = (entry_price * 1.02 - entry_price) * position_size  # Assume 2% target
            else:  # SHORT
                potential_profit = (entry_price - entry_price * 0.98) * position_size
            
            risk_reward = potential_profit / risk_amount if risk_amount > 0 else 0
            min_rr = self.config.get('risk_reward_min', 1.5)
            
            if risk_reward < min_rr:
                logger.warning(f"Position rejected: Risk/Reward too low ({risk_reward:.2f} < {min_rr})")
                return {
                    'position_size': 0,
                    'risk_amount': 0,
                    'risk_percent': 0,
                    'approved': False,
                    'reason': f"Risk/Reward too low ({risk_reward:.2f} < {min_rr})"
                }
            
            logger.info(f"Position approved: Size={position_size:.4f}, Risk=${risk_amount:.2f} ({risk_percent:.2f}%)")
            
            return {
                'position_size': round(position_size, 8),
                'risk_amount': round(risk_amount, 2),
                'risk_percent': round(risk_percent, 2),
                'risk_reward': round(risk_reward, 2),
                'approved': True,
                'reason': "Approved"
            }
    
    def update_pnl(self, pnl: float):
        """Update P&L after a trade"""
        with self.lock:
            self.total_pnl += pnl
            self.daily_pnl += pnl
            self.account_balance += pnl
            
            # Update peak balance and drawdown
            if self.account_balance > self.peak_balance:
                self.peak_balance = self.account_balance
            
            self.current_drawdown = ((self.peak_balance - self.account_balance) / self.peak_balance) * 100
            
            logger.debug(f"P&L updated: {pnl:+.2f}, Balance: ${self.account_balance:.2f}, Drawdown: {self.current_drawdown:.2f}%")
    
    def increment_trades(self):
        """Increment daily trade count"""
        with self.lock:
            self.daily_trades += 1
            logger.debug(f"Daily trades incremented: {self.daily_trades}")
    
    def reset_daily(self):
        """Reset daily counters (call at start of new day)"""
        with self.lock:
            today = datetime.now().date()
            if today > self.last_reset_day:
                self.daily_trades = 0
                self.daily_pnl = 0.0
                self.last_reset_day = today
                logger.info(f"Daily counters reset for {today}")
    
    def kill_switch(self, reason: str):
        """Emergency stop - kills all trading"""
        with self.lock:
            self.is_killed = True
            self.kill_reason = reason
            logger.critical(f"KILL SWITCH ACTIVATED: {reason}")
    
    def get_status(self) -> Dict:
        """Get current risk status"""
        with self.lock:
            status = {
                'account_balance': round(self.account_balance, 2),
                'total_pnl': round(self.total_pnl, 2),
                'daily_pnl': round(self.daily_pnl, 2),
                'daily_trades': self.daily_trades,
                'current_drawdown': round(self.current_drawdown, 2),
                'peak_balance': round(self.peak_balance, 2),
                'is_killed': self.is_killed,
                'kill_reason': self.kill_reason,
                'daily_loss_percent': round((self.daily_pnl / self.initial_balance) * 100, 2),
                'max_positions': self.max_positions
            }
            
            logger.debug(f"Risk status requested: Balance=${status['account_balance']}, Drawdown={status['current_drawdown']}%")
            return status
    
    def update_config(self, new_config: Dict):
        """Update risk parameters"""
        with self.lock:
            old_config = self.config.copy()
            self.config.update(new_config)
            self.max_positions = self.config.get('max_open_positions', 5)
            self._save_config()
            logger.info(f"Risk config updated: {old_config} -> {self.config}")
    
    def _save_config(self):
        """Save current config to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.debug(f"Risk config saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")


if __name__ == "__main__":
    # Test the risk manager
    rm = RiskManager(account_balance=10000)
    
    logger.info("\nTesting Risk Manager...")
    
    # Test position sizing
    result = rm.calculate_position_size(
        entry_price=50000,
        stop_loss=49000,
        confidence=0.85
    )
    
    logger.info("\nPosition Sizing Result:")
    logger.info(f"  Approved: {result['approved']}")
    logger.info(f"  Reason: {result['reason']}")
    logger.info(f"  Position Size: {result.get('position_size', 0):.4f}")
    logger.info(f"  Risk Amount: ${result.get('risk_amount', 0):.2f}")
    logger.info(f"  Risk %: {result.get('risk_percent', 0):.2f}%")
    
    # Test status
    logger.info("\nCurrent Status:")
    status = rm.get_status()
    for key, value in status.items():
        logger.info(f"  {key}: {value}")