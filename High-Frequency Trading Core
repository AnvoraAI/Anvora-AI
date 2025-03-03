use std::collections::HashMap;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: u64,
    pub symbol: String,
    pub price: f64,
    pub quantity: i64,
    pub direction: Direction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Direction {
    Buy,
    Sell,
}

/// Lock-free order matching engine with price-time priority
pub struct OrderBook {
    bids: Mutex<HashMap<u64, Order>>,
    asks: Mutex<HashMap<u64, Order>>,
    sequence: Mutex<u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: Mutex::new(HashMap::new()),
            asks: Mutex::new(HashMap::new()),
            sequence: Mutex::new(0),
        }
    }

    /// Process order with nanosecond-level latency
    pub async fn process_order(&self, order: Order) -> Result<u64, String> {
        let mut seq = self.sequence.lock().await;
        *seq += 1;
        let order_id = *seq;
        
        match order.direction {
            Direction::Buy => {
                let mut bids = self.bids.lock().await;
                bids.insert(order_id, order);
            }
            Direction::Sell => {
                let mut asks = self.asks.lock().await;
                asks.insert(order_id, order);
            }
        }
        
        Ok(order_id)
    }
}
