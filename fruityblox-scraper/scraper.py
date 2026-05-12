#!/usr/bin/env python3
"""
FruityBlox Stock Monitor - API Poller
Fetches stock data from GitHub API and sends Discord notifications on rotation changes.
"""
import sys
import os
import time
import hashlib
import logging
import requests
import yaml
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# Add shared module to path
sys.path.insert(0, '/opt/services/shared')
import db

# Load config
with open('config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

# Setup logging (stdout only, supervisor redirects to log file)
logging.basicConfig(
    level=getattr(logging, CONFIG['log_level']),
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# API endpoints
STOCK_API = CONFIG['stock_api']
FRUITS_API = CONFIG['fruits_api']


def fetch_stock_data():
    """Fetch stock data from GitHub API with retry on empty data."""
    max_retries = 3
    retry_delay = 30  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(STOCK_API, timeout=CONFIG['request_timeout'],
                                    headers={'User-Agent': 'FruityBlox-Monitor/1.0',
                                             'Cache-Control': 'no-cache'})
            response.raise_for_status()
            data = response.json()
            
            normal = data.get('normal', [])
            mirage = data.get('mirage', [])
            
            # Check if data is empty (API transitioning)
            if len(normal) == 0 and len(mirage) == 0:
                if attempt < max_retries:
                    logger.warning(f"⚠ API returned empty data (attempt {attempt}/{max_retries}), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.warning(f"⚠ API still empty after {max_retries} attempts, skipping this poll")
                    return None
            
            logger.info(f"✓ Fetched stock data: {len(normal)} normal, {len(mirage)} mirage")
            return data
            
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"⚠ Fetch failed (attempt {attempt}/{max_retries}): {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"✗ Failed to fetch stock data after {max_retries} attempts: {e}")
                raise
    
    return None


def fetch_fruits_metadata():
    """Fetch fruit metadata (rarity, robux, image) from API."""
    try:
        response = requests.get(FRUITS_API, timeout=CONFIG['request_timeout'])
        response.raise_for_status()
        data = response.json()
        logger.info(f"✓ Fetched metadata for {len(data)} fruits")
        return data
    except Exception as e:
        logger.warning(f"⚠ Failed to fetch fruits metadata: {e}")
        return {}


def generate_rotation_id(fruits_list):
    """Generate unique ID for rotation based on fruit names."""
    fruit_names = sorted([f['name'] for f in fruits_list])
    rotation_str = '|'.join(fruit_names)
    return hashlib.md5(rotation_str.encode()).hexdigest()


def enrich_stock_data(stock_data, fruits_metadata):
    """Add rarity, robux, image from metadata to stock data."""
    enriched = []
    for fruit in stock_data:
        # Extract fruit key from "Rocket-Rocket" -> "Rocket"
        fruit_key = fruit['name'].split('-')[0]
        metadata = fruits_metadata.get(fruit_key, {})
        
        enriched.append({
            'name': fruit['name'],
            'price': fruit['price'],
            'rarity': metadata.get('rarity', 'unknown'),
            'robux': metadata.get('robux', 0),
            'image_url': metadata.get('imageURL', '')
        })
    
    return enriched


def check_rotation_change(conn, stock_type, current_rotation_id):
    """Check if rotation has changed."""
    last_rotation_id = db.get_fruityblox_config(conn, f'last_{stock_type}_rotation_id')
    is_new = current_rotation_id != last_rotation_id
    
    if is_new:
        logger.info(f"🔄 New {stock_type} rotation detected! ID: {current_rotation_id[:8]}...")
    else:
        logger.info(f"✓ {stock_type.title()} rotation unchanged")
    
    return is_new


def build_discord_embed(stock_type, fruits, updated_at=None):
    """Build Discord rich embed for stock notification."""
    color = 0x3498db if stock_type == 'normal' else 0x9b59b6

    # Group by rarity
    grouped = {}
    for fruit in fruits:
        rarity = fruit.get('rarity', 'unknown')
        grouped.setdefault(rarity, []).append(fruit)

    # Build fields
    fields = []
    rarity_order = ['mythical', 'legendary', 'rare', 'uncommon', 'common', 'unknown']
    rarity_emojis = {
        'mythical': '🔥', 'legendary': '⭐', 'rare': '💎',
        'uncommon': '🌟', 'common': '⚪', 'unknown': '❓'
    }

    for rarity in rarity_order:
        if rarity in grouped:
            lines = []
            for f in sorted(grouped[rarity], key=lambda x: x.get('price', x.get('price_beli', 0)), reverse=True):
                name = f.get('name', f.get('fruit_name', '?'))
                price = f.get('price', f.get('price_beli', 0))
                robux = f.get('robux', f.get('price_robux', 0))
                robux_str = f" | 💎 {robux:,} Robux" if robux else ""
                lines.append(f"• **{name}** — 💰 {price:,} Beli{robux_str}")
            fields.append({
                'name': f"{rarity_emojis[rarity]} {rarity.title()}",
                'value': '\n'.join(lines),
                'inline': False
            })

    # Parse updated_at and calculate next rotation
    now = datetime.now()
    if updated_at:
        try:
            # Parse ISO format from GitHub API e.g. "2026-05-09T21:25:17.030Z"
            from datetime import timezone
            dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            # Convert to local time
            dt_local = dt.astimezone().replace(tzinfo=None)
            next_update = dt_local + timedelta(hours=4)
            diff = next_update - now
            if diff.total_seconds() > 0:
                h = int(diff.total_seconds() // 3600)
                m = int((diff.total_seconds() % 3600) // 60)
                next_str = f"{h}j {m}m lagi" if h > 0 else f"{m}m lagi"
            else:
                next_str = "Segera"
            updated_str = dt_local.strftime('%d %b %Y, %H:%M WIB')
        except Exception:
            updated_str = now.strftime('%d %b %Y, %H:%M WIB')
            next_str = "~4 jam"
    else:
        updated_str = now.strftime('%d %b %Y, %H:%M WIB')
        next_str = "~4 jam"

    embed = {
        'title': f"{'🍎' if stock_type == 'normal' else '✨'} Blox Fruits Stock — {stock_type.title()}",
        'description': f"⏰ **Update:** {updated_str}\n⏭️ **Next rotation:** {next_str}",
        'color': color,
        'fields': fields,
        'footer': {'text': f"📊 {len(fruits)} fruits • FruityBlox Monitor"},
        'timestamp': datetime.utcnow().isoformat()
    }

    return embed


def send_discord_notification(conn, stock_type, fruits, rotation_id, updated_at=None):
    """Send Discord webhook notification."""
    webhook_url = db.get_fruityblox_config(conn, 'discord_webhook_url')
    
    if not webhook_url:
        logger.warning("⚠ Discord webhook URL not configured, skipping notification")
        return
    
    # Check if notifications enabled for this stock type
    notify_enabled = db.get_fruityblox_config(conn, f'notify_{stock_type}')
    if notify_enabled != '1':
        logger.info(f"ℹ Notifications disabled for {stock_type}, skipping")
        return
    
    try:
        # Build embed with updated_at for next rotation calculation
        embed = build_discord_embed(stock_type, fruits, updated_at=updated_at)
        
        # Get mentions
        mentions = db.get_fruityblox_config(conn, 'discord_mentions')
        content = ""
        if mentions:
            role_ids = [rid.strip() for rid in mentions.split(',') if rid.strip()]
            content = ' '.join([f"<@&{rid}>" for rid in role_ids])
        
        # Send webhook
        payload = {
            'content': content,
            'embeds': [embed]
        }
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers={'User-Agent': 'FruityBlox-Monitor/1.0'},
            timeout=10
        )
        response.raise_for_status()
        
        logger.info(f"✓ Discord notification sent for {stock_type} stock")
        
        # Mark as notified
        db.update_fruityblox_rotation_notified(conn, rotation_id)
        
    except Exception as e:
        logger.error(f"✗ Failed to send Discord notification: {e}")


def process_stock_type(conn, stock_type, stock_data, fruits_metadata, updated_at=None):
    """Process a single stock type (normal or mirage)."""
    start_time = time.time()
    
    try:
        # Skip if stock_data is empty
        if not stock_data:
            logger.warning(f"⚠ {stock_type.title()} stock data is empty, skipping")
            duration = time.time() - start_time
            db.log_fruityblox_scrape_run(
                conn, stock_type, 0, False, '', duration, 'skipped', 'Empty data from API'
            )
            return False
        
        # Enrich stock data with metadata
        fruits = enrich_stock_data(stock_data, fruits_metadata)
        
        # Generate rotation ID
        rotation_id = generate_rotation_id(stock_data)
        
        # Check if rotation changed
        is_new_rotation = check_rotation_change(conn, stock_type, rotation_id)
        
        if is_new_rotation:
            # Save to database
            db.save_fruityblox_stock(conn, stock_type, fruits, rotation_id)
            db.save_fruityblox_rotation(conn, rotation_id, stock_type, len(fruits))
            
            # Update last rotation ID
            db.set_fruityblox_config(conn, f'last_{stock_type}_rotation_id', rotation_id)
            
            # Send Discord notification with updated_at for next rotation time
            send_discord_notification(conn, stock_type, fruits, rotation_id, updated_at=updated_at)
            
            logger.info(f"✓ Saved {len(fruits)} {stock_type} fruits to database")
        
        # Log scrape run
        duration = time.time() - start_time
        db.log_fruityblox_scrape_run(
            conn, stock_type, len(fruits), is_new_rotation,
            rotation_id, duration, 'success'
        )
        
        return True
        
    except Exception as e:
        duration = time.time() - start_time
        db.log_fruityblox_scrape_run(
            conn, stock_type, 0, False, '', duration, 'failed', str(e)
        )
        logger.error(f"✗ Failed to process {stock_type} stock: {e}")
        return False


def poll_stock():
    """Main polling function - fetches and processes stock data."""
    logger.info("=" * 60)
    logger.info("🍎 Starting FruityBlox stock poll...")
    logger.info("=" * 60)
    
    conn = db.get_db()
    
    try:
        # Fetch data from APIs
        stock_data = fetch_stock_data()
        
        # Skip if API returned empty/None
        if stock_data is None:
            logger.warning("⚠ Stock data unavailable, skipping this poll cycle")
            return
        
        fruits_metadata = fetch_fruits_metadata()
        updated_at = stock_data.get('updated_at')
        
        # Process Normal stock (only if data exists)
        normal_data = stock_data.get('normal', [])
        if normal_data:
            logger.info(f"📦 Processing Normal stock ({len(normal_data)} fruits)...")
            process_stock_type(conn, 'normal', normal_data, fruits_metadata, updated_at=updated_at)
        else:
            logger.warning("⚠ Normal stock empty, skipping")
        
        # Process Mirage stock (only if data exists)
        mirage_data = stock_data.get('mirage', [])
        if mirage_data:
            logger.info(f"✨ Processing Mirage stock ({len(mirage_data)} fruits)...")
            process_stock_type(conn, 'mirage', mirage_data, fruits_metadata, updated_at=updated_at)
        else:
            logger.warning("⚠ Mirage stock empty, skipping")
        
        logger.info("✓ Stock poll completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Stock poll failed: {e}")
    
    finally:
        conn.close()
    
    logger.info("=" * 60)


def main():
    """Main entry point."""
    check_minutes = CONFIG.get('check_interval_minutes', 30)
    
    logger.info("🚀 FruityBlox Stock Monitor starting...")
    logger.info(f"📍 Database: {CONFIG['db_path']}")
    logger.info(f"⏰ Check interval: {check_minutes} minutes")
    
    # Run immediately on startup
    logger.info("🔄 Running initial stock check...")
    poll_stock()
    
    # Setup scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(
        poll_stock,
        'interval',
        minutes=check_minutes,
        id='fruityblox_poll',
        name='FruityBlox Stock Poll'
    )
    
    logger.info(f"✓ Scheduler configured: every {check_minutes} minutes")
    logger.info("🎯 Service is running. Press Ctrl+C to stop.")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Shutting down gracefully...")
        scheduler.shutdown()


if __name__ == '__main__':
    main()
