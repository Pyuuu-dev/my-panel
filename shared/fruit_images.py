"""
Fruit image mapping for FruityBlox
Uses reliable CDN sources for fruit images
"""

# Fruit emoji mapping as fallback
FRUIT_EMOJIS = {
    'rocket': '🚀',
    'spin': '🌀',
    'chop': '✂️',
    'spring': '🌸',
    'bomb': '💣',
    'smoke': '💨',
    'spike': '🔱',
    'flame': '🔥',
    'falcon': '🦅',
    'ice': '❄️',
    'sand': '🏖️',
    'dark': '🌑',
    'diamond': '💎',
    'light': '✨',
    'rubber': '🎈',
    'barrier': '🛡️',
    'ghost': '👻',
    'magma': '🌋',
    'quake': '💥',
    'buddha': '🧘',
    'love': '💗',
    'spider': '🕷️',
    'sound': '🔊',
    'phoenix': '🔥',
    'portal': '🌀',
    'rumble': '⚡',
    'pain': '💢',
    'blizzard': '🌨️',
    'gravity': '🌌',
    'mammoth': '🦣',
    'dough': '🍩',
    'shadow': '👤',
    'venom': '☠️',
    'control': '🎮',
    'spirit': '👻',
    'dragon': '🐉',
    'leopard': '🐆',
    'kitsune': '🦊',
    'trex': '🦖',
}

def get_fruit_emoji(fruit_name):
    """Get emoji for fruit name."""
    # Extract base name (e.g., "Rocket-Rocket" -> "rocket")
    base_name = fruit_name.split('-')[0].lower()
    return FRUIT_EMOJIS.get(base_name, '🍎')

def get_fruit_image_url(fruit_name):
    """
    Get image URL for fruit.
    Returns emoji as fallback since Wikia images are not directly accessible.
    """
    # For now, return None to use emoji fallback in template
    # In future, can add CDN URLs here
    return None
