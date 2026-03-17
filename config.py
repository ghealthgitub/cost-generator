"""
🫚 Ginger Universe — Cost Generator Configuration
All secrets loaded from environment variables
"""
import os

# ── Database (shared with profile generator + admin) ─────────
DATABASE_URL = os.environ.get('DATABASE_URL', '')

# ── Claude API ───────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

# ── Application ──────────────────────────────────────────────
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32).hex())
ADMIN_URL = os.environ.get('ADMIN_URL', 'https://enter.ginger.healthcare')
