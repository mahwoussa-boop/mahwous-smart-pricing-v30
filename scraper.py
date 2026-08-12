import asyncio
import json
import re
import random
import logging
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from scrape_compliance import ScrapePolicy

logger = logging.getLogger(__name__)


class PerfumeScraper:
    """متصفح للصفحات العامة المسموح بسحبها من المتاجر السعودية."""

    def __init__(self, stores_config_path='stores_config.json'):
        self.stores_config = self._load_config(stores_config_path)
        self.scrape_policy = ScrapePolicy(min_interval_seconds=5.0)
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None

    def _load_config(self, path):
        """Load stores configuration from JSON file"""
        config_path = Path(path)
        if not config_path.is_absolute():
            # Look relative to this script's directory
            config_path = Path(__file__).parent / path
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f'Config file not found: {config_path}, using empty config')
            return {'stores': []}
        except json.JSONDecodeError as e:
            logger.error(f'Invalid JSON in config: {e}')
            return {'stores': []}

    def _get_store(self, store_name):
        """Get store config by name or name_en"""
        for store in self.stores_config.get('stores', []):
            if store.get('name') == store_name or store.get('name_en') == store_name:
                return store
        return None

    async def start_browser(self, headless=False):
        """شغّل متصفحاً معلناً بلا إخفاء للأتمتة أو تجاوز لحماية الموقع."""
        logger.info('🚀 Starting browser...')
        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=headless,
            args=[
                '--lang=ar-SA',
                '--window-size=1366,768',
            ]
        )

        headers = self.scrape_policy.headers()
        user_agent = headers.pop('User-Agent')
        logger.info('Using declared research User-Agent')

        self.context = await self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent=user_agent,
            locale='ar-SA',
            timezone_id='Asia/Riyadh',
            permissions=['geolocation'],
            geolocation={'latitude': 24.7136, 'longitude': 46.6753},  # Riyadh
            color_scheme='light',
            java_script_enabled=True,
            extra_http_headers=headers,
        )

        self.page = await self.context.new_page()
        logger.info('✅ Browser started successfully')

    async def close_browser(self):
        """Close browser and cleanup resources"""
        try:
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            logger.info('🔒 Browser closed')
        except Exception as e:
            logger.error(f'Error closing browser: {e}')

    async def _random_delay(self, min_sec=2, max_sec=5):
        """Random delay to appear human-like"""
        delay = random.uniform(min_sec, max_sec)
        logger.debug(f'⏳ Waiting {delay:.1f}s...')
        await asyncio.sleep(delay)

    async def _handle_cloudflare(self, timeout=30000):
        """
        Detect an anti-bot challenge and report it to the caller.

        لا ننتظر محاولة الحل ولا نغير خصائص المتصفح لتجاوز الحماية. يجب على
        المستخدم الرجوع لشروط الموقع أو استخدام واجهة بيانات رسمية متى توفرت.
        """
        cf_indicators = [
            'text="Checking your browser"',
            'text="Just a moment"',
            'text="يتم التحقق من متصفحك"',
            '#challenge-running',
            '#cf-challenge-running',
            '.cf-browser-verification',
        ]

        for indicator in cf_indicators:
            try:
                el = await self.page.query_selector(indicator)
                if el:
                    logger.warning('☁️ Cloudflare challenge detected; stopping this request')
                    return True
            except Exception:
                continue

        return False

    async def _safe_click(self, selector, timeout=10000):
        """Click element with error handling and retry (3 attempts)"""
        for attempt in range(3):
            try:
                await self.page.wait_for_selector(selector, state='visible', timeout=timeout)
                await self._random_delay(0.5, 1.5)
                await self.page.click(selector)
                logger.info(f'✅ Clicked: {selector}')
                return True
            except PlaywrightTimeout:
                logger.warning(f'⚠️ Attempt {attempt + 1}/3: Selector not found: {selector}')
                if attempt < 2:
                    await self._random_delay(2, 4)
            except Exception as e:
                logger.error(f'❌ Click error on attempt {attempt + 1}/3: {e}')
                if attempt < 2:
                    await self._random_delay(2, 4)
        return False

    async def _safe_fill(self, selector, value, timeout=10000):
        """Fill input field safely with retry"""
        for attempt in range(3):
            try:
                await self.page.wait_for_selector(selector, state='visible', timeout=timeout)
                await self._random_delay(0.3, 0.8)
                await self.page.fill(selector, value)
                logger.info(f'✅ Filled: {selector}')
                return True
            except PlaywrightTimeout:
                logger.warning(f'⚠️ Attempt {attempt + 1}/3: Input not found: {selector}')
                if attempt < 2:
                    await self._random_delay(1, 3)
            except Exception as e:
                logger.error(f'❌ Fill error: {e}')
                if attempt < 2:
                    await self._random_delay(1, 3)
        return False

    async def _safe_get_text(self, selector, timeout=5000):
        """Get text content safely with fallback"""
        try:
            el = await self.page.wait_for_selector(selector, timeout=timeout)
            if el:
                text = await el.text_content()
                return text.strip() if text else ''
        except Exception:
            pass
        return ''

    async def _safe_get_attr(self, selector, attr, timeout=5000):
        """Get element attribute safely"""
        try:
            el = await self.page.wait_for_selector(selector, timeout=timeout)
            if el:
                return await el.get_attribute(attr) or ''
        except Exception:
            pass
        return ''

    async def _scroll_page(self, scrolls=3):
        """Simulate human scrolling to trigger lazy-loaded content"""
        for i in range(scrolls):
            await self.page.evaluate(
                f'window.scrollTo({{top: document.body.scrollHeight * {(i + 1) / scrolls}, behavior: "smooth"}})'
            )
            await self._random_delay(1, 2.5)
        # Scroll back to top
        await self.page.evaluate('window.scrollTo({top: 0, behavior: "smooth"})')
        await self._random_delay(0.5, 1)

    async def _extract_product_from_element(self, el, selectors, base_url, rank):
        """Extract product data from a single product card element"""
        name = ''
        price = ''
        link = ''
        image = ''

        # --- Extract name ---
        name_selectors = selectors.get('product_name', 'h2,h3,.product-title,.product-name').split(',')
        for name_sel in name_selectors:
            try:
                name_el = await el.query_selector(name_sel.strip())
                if name_el:
                    name = (await name_el.text_content() or '').strip()
                    if name:
                        break
            except Exception:
                continue

        # --- Extract price ---
        price_selectors = selectors.get('product_price', '.price,.product-price,.amount').split(',')
        for price_sel in price_selectors:
            try:
                price_el = await el.query_selector(price_sel.strip())
                if price_el:
                    price = (await price_el.text_content() or '').strip()
                    if price:
                        break
            except Exception:
                continue

        # --- Extract link ---
        link_selectors = selectors.get('product_link', 'a').split(',')
        for link_sel in link_selectors:
            try:
                link_el = await el.query_selector(link_sel.strip())
                if link_el:
                    link = await link_el.get_attribute('href') or ''
                    if link and not link.startswith('http'):
                        link = base_url.rstrip('/') + ('/' if not link.startswith('/') else '') + link.lstrip('/')
                    if link:
                        break
            except Exception:
                continue

        # Fallback: try first <a> in the element
        if not link:
            try:
                link_el = await el.query_selector('a')
                if link_el:
                    link = await link_el.get_attribute('href') or ''
                    if link and not link.startswith('http'):
                        link = base_url.rstrip('/') + ('/' if not link.startswith('/') else '') + link.lstrip('/')
            except Exception:
                pass

        # --- Extract image ---
        image_selectors = selectors.get('product_image', 'img').split(',')
        for img_sel in image_selectors:
            try:
                img_el = await el.query_selector(img_sel.strip())
                if img_el:
                    image = (
                        await img_el.get_attribute('src')
                        or await img_el.get_attribute('data-src')
                        or await img_el.get_attribute('data-lazy-src')
                        or await img_el.get_attribute('data-original')
                        or ''
                    )
                    if image:
                        break
            except Exception:
                continue

        # --- Clean price ---
        price_clean = ''
        if price:
            price_nums = re.findall(r'[\d,\.]+', price)
            if price_nums:
                price_clean = price_nums[0].replace(',', '')

        return {
            'name': name,
            'price': float(price_clean) if price_clean else 0,
            'price_display': price,
            'url': link,
            'image_url': image,
            'trend_rank': rank,
        }

    async def scrape_trending(self, store_name, custom_url=None, max_products=10):
        """
        Scrape trending/bestselling products from a store.

        Args:
            store_name: Store identifier (name or name_en from config)
            custom_url: Optional custom URL to scrape instead of configured URL
            max_products: Maximum number of products to return

        Returns:
            dict with 'products' list, 'count', and 'store' name
        """
        store = self._get_store(store_name)
        if not store and not custom_url:
            logger.error(f'Store "{store_name}" not found in config')
            return {'error': f'Store {store_name} not found', 'products': []}

        if custom_url:
            # Use generic Salla config as fallback for custom URLs
            fallback_store = self._get_store('generic_salla') or {
                'selectors': {
                    'product_card': '.product-item, .product-card, [class*="product"]',
                    'product_name': 'h2, h3, .product-title, .product-name',
                    'product_price': '.price, .product-price, .amount',
                    'product_link': 'a',
                    'product_image': 'img',
                }
            }
            if store:
                store = {**store, 'base_url': custom_url.rstrip('/')}
            else:
                store = {**fallback_store, 'base_url': custom_url.rstrip('/'), 'name': store_name}
            trending_url = custom_url
        else:
            trending_url = store.get('trending_url', store['base_url'])

        products = []

        try:
            decision = await asyncio.to_thread(self.scrape_policy.check, trending_url)
            if not decision.allowed:
                return {
                    'error': f'لم يبدأ السحب: {decision.reason}',
                    'products': [],
                    'store': store.get('name', store_name),
                }
            await asyncio.to_thread(self.scrape_policy.wait_for_slot, trending_url)
            if not self.browser:
                await self.start_browser(headless=False)

            logger.info(f'🔍 Navigating to: {trending_url}')
            await self.page.goto(trending_url, wait_until='domcontentloaded', timeout=30000)
            await self._random_delay(3, 6)

            if await self._handle_cloudflare():
                return {
                    'error': 'أوقف الموقع الطلب للتحقق؛ لم تتم محاولة تجاوزه',
                    'products': [],
                    'store': store.get('name', store_name),
                }

            # Scroll to trigger lazy loading
            await self._scroll_page(scrolls=3)

            # Wait for products to load
            selectors = store.get('selectors', {})
            product_selector = selectors.get('product_card', '.product-item')

            try:
                await self.page.wait_for_selector(product_selector, timeout=15000)
                logger.info(f'✅ Found products with selector: {product_selector}')
            except PlaywrightTimeout:
                # Try alternative selectors for different platforms
                alt_selectors = [
                    '.product-item',           # Salla
                    '.product-card',           # Generic
                    '.product-entry',          # Zid
                    '[class*="product"]',      # Fuzzy match
                    '.grid-item',              # Grid layouts
                    '.col-product',            # Column layouts
                    '.woocommerce-loop-product__link',  # WooCommerce
                    '.product-block',          # Shopify
                    '.product',                # Generic
                    'li.product',              # WooCommerce list
                ]
                found = False
                for alt in alt_selectors:
                    try:
                        await self.page.wait_for_selector(alt, timeout=5000)
                        product_selector = alt
                        logger.info(f'✅ Found products with alternative selector: {alt}')
                        found = True
                        break
                    except PlaywrightTimeout:
                        continue

                if not found:
                    logger.warning('⚠️ No product elements found with any known selector')
                    # Take a screenshot for debugging
                    try:
                        screenshot_path = Path(__file__).parent / 'debug_screenshot.png'
                        await self.page.screenshot(path=str(screenshot_path))
                        logger.info(f'📸 Debug screenshot saved: {screenshot_path}')
                    except Exception:
                        pass
                    return {
                        'error': 'لم يتم العثور على منتجات - No products found',
                        'products': [],
                        'store': store.get('name', store_name),
                    }

            # Get all product elements
            product_elements = await self.page.query_selector_all(product_selector)
            logger.info(f'📦 Found {len(product_elements)} product elements')

            for i, el in enumerate(product_elements[:max_products]):
                try:
                    product = await self._extract_product_from_element(
                        el,
                        selectors,
                        store.get('base_url', trending_url),
                        rank=i + 1,
                    )
                    product['store'] = store.get('name', store_name)

                    if product['name']:  # Only add if we got a name
                        products.append(product)
                        logger.info(f'  #{i + 1}: {product["name"][:40]} - {product["price_display"]}')
                except Exception as e:
                    logger.warning(f'⚠️ Error extracting product {i + 1}: {e}')
                    continue

            logger.info(f'✅ Scraped {len(products)} products from {store.get("name", store_name)}')
            return {
                'products': products,
                'count': len(products),
                'store': store.get('name', store_name),
            }

        except PlaywrightTimeout:
            logger.error('❌ Page load timeout')
            return {
                'error': 'Page load timeout - الموقع لم يستجب',
                'products': products,
                'store': store.get('name', store_name),
            }
        except Exception as e:
            logger.error(f'❌ Scraping error: {e}')
            return {
                'error': str(e),
                'products': products,
                'store': store.get('name', store_name),
            }

    async def add_to_cart(self, product_url, store_name):
        """
        Navigate to product page and add to cart.
        ⚠️ Stops BEFORE checkout — never completes payment.

        Args:
            product_url: Full URL of the product page
            store_name: Store identifier for selector lookup

        Returns:
            dict with success status and message
        """
        store = self._get_store(store_name)
        if not store:
            store = self._get_store('generic_salla') or {
                'selectors': {
                    'add_to_cart': '.add-to-cart, button[class*="cart"]',
                }
            }

        try:
            decision = await asyncio.to_thread(self.scrape_policy.check, product_url)
            if not decision.allowed:
                return {'success': False, 'message': f'لم يبدأ الطلب: {decision.reason}'}
            await asyncio.to_thread(self.scrape_policy.wait_for_slot, product_url)
            if not self.browser:
                await self.start_browser(headless=False)

            logger.info(f'🛒 Opening product: {product_url}')
            await self.page.goto(product_url, wait_until='domcontentloaded', timeout=30000)
            await self._random_delay(3, 5)

            if await self._handle_cloudflare():
                return {
                    'success': False,
                    'message': 'أوقف الموقع الطلب للتحقق؛ لم تتم محاولة تجاوزه',
                }

            # Try to click Add to Cart
            selectors = store.get('selectors', {})
            add_cart_selector = selectors.get('add_to_cart', '.add-to-cart')

            success = await self._safe_click(add_cart_selector)

            if not success:
                # Try alternative selectors across platforms
                alt_selectors = [
                    'button[class*="cart"]',
                    '.add-to-cart',
                    '[class*="add-cart"]',
                    'button:has-text("أضف")',
                    'button:has-text("السلة")',
                    'button:has-text("اضف")',
                    'button:has-text("أضف للسلة")',
                    'button:has-text("إضافة للسلة")',
                    'button:has-text("Add to cart")',
                    'button:has-text("Add to Cart")',
                    '.btn-cart',
                    '#add-to-cart',
                    '.single_add_to_cart_button',     # WooCommerce
                    'button[name="add-to-cart"]',     # WooCommerce
                    '.product-add-btn',               # Zid
                    '[data-action="addToCart"]',       # Salla
                ]
                for alt in alt_selectors:
                    success = await self._safe_click(alt, timeout=5000)
                    if success:
                        logger.info(f'✅ Added to cart using selector: {alt}')
                        break

            if success:
                await self._random_delay(2, 4)
                logger.info('✅ Product added to cart successfully')
                return {
                    'success': True,
                    'message': 'تمت الإضافة للسلة - المتصفح مفتوح عند صفحة المنتج',
                    'status': 'in_cart',
                }
            else:
                logger.warning('⚠️ Add to cart button not found')
                return {
                    'success': False,
                    'message': 'لم يتم العثور على زر الإضافة للسلة - المتصفح مفتوح للإضافة اليدوية',
                    'status': 'pending',
                }

        except PlaywrightTimeout:
            logger.error('❌ Timeout while adding to cart')
            return {'success': False, 'message': 'انتهت مهلة تحميل الصفحة', 'status': 'error'}
        except Exception as e:
            logger.error(f'❌ Add to cart error: {e}')
            return {'success': False, 'message': str(e), 'status': 'error'}

    async def goto_checkout(self, store_name):
        """
        Navigate to checkout page.
        ⚠️ Does NOT complete purchase — stops at checkout for manual completion.

        Args:
            store_name: Store identifier

        Returns:
            dict with success status
        """
        store = self._get_store(store_name) or self._get_store('generic_salla') or {
            'selectors': {
                'cart_icon': '.cart-icon, [class*="cart"]',
                'checkout_button': '.checkout-btn, [class*="checkout"]',
            }
        }
        selectors = store.get('selectors', {})

        try:
            # Click cart icon
            cart_sel = selectors.get('cart_icon', '.cart-icon')
            cart_clicked = await self._safe_click(cart_sel)
            if not cart_clicked:
                # Try alternatives
                for alt in ['.cart-icon', '[class*="cart-icon"]', '.header-cart', '#cart-icon', 'a[href*="cart"]']:
                    if await self._safe_click(alt, timeout=5000):
                        break
            await self._random_delay(2, 3)

            # Click checkout button
            checkout_sel = selectors.get('checkout_button', '.checkout-btn')
            checkout_clicked = await self._safe_click(checkout_sel)
            if not checkout_clicked:
                for alt in ['.checkout-btn', '[class*="checkout"]', 'a[href*="checkout"]',
                            'button:has-text("إتمام")', 'button:has-text("الدفع")',
                            'button:has-text("Checkout")']:
                    if await self._safe_click(alt, timeout=5000):
                        checkout_clicked = True
                        break
            await self._random_delay(2, 3)

            if checkout_clicked:
                logger.info('✅ Reached checkout page — STOPPING here for manual completion')
                return {
                    'success': True,
                    'message': 'وصلت لصفحة الدفع - أكمل الدفع يدوياً ⚠️',
                    'status': 'checkout',
                }
            else:
                logger.warning('⚠️ Could not reach checkout automatically')
                return {
                    'success': False,
                    'message': 'لم يتم الوصول لصفحة الدفع - حاول يدوياً',
                    'status': 'in_cart',
                }
        except Exception as e:
            logger.error(f'❌ Checkout navigation error: {e}')
            return {
                'success': False,
                'message': f'خطأ في الوصول للدفع: {e}',
                'status': 'in_cart',
            }

    async def search_products(self, store_name, query, max_products=10):
        """
        Search for products in a store.

        Args:
            store_name: Store identifier
            query: Search query (e.g. "عود", "مسك")
            max_products: Max results to return

        Returns:
            dict with products list
        """
        store = self._get_store(store_name)
        if not store:
            return {'error': f'Store {store_name} not found', 'products': []}

        try:
            if not self.browser:
                await self.start_browser(headless=False)

            # Build search URL
            search_url = store.get('search_url', f'{store["base_url"]}/search?q=')
            if '{query}' in search_url:
                search_url = search_url.replace('{query}', query)
            else:
                search_url = search_url + query

            logger.info(f'🔍 Searching: {search_url}')
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._random_delay(3, 6)

            await self._handle_cloudflare()
            await self._scroll_page(scrolls=2)

            # Reuse trending scraper logic for extracting products
            selectors = store.get('selectors', {})
            product_selector = selectors.get('product_card', '.product-item')

            product_elements = []
            try:
                await self.page.wait_for_selector(product_selector, timeout=10000)
                product_elements = await self.page.query_selector_all(product_selector)
            except PlaywrightTimeout:
                # Fallback selectors
                for alt in ['.product-item', '.product-card', '[class*="product"]', '.search-result-item']:
                    try:
                        await self.page.wait_for_selector(alt, timeout=5000)
                        product_elements = await self.page.query_selector_all(alt)
                        if product_elements:
                            break
                    except PlaywrightTimeout:
                        continue

            products = []
            for i, el in enumerate(product_elements[:max_products]):
                try:
                    product = await self._extract_product_from_element(
                        el, selectors, store.get('base_url', ''), rank=i + 1
                    )
                    product['store'] = store.get('name', store_name)
                    if product['name']:
                        products.append(product)
                except Exception as e:
                    logger.warning(f'⚠️ Error extracting search result {i + 1}: {e}')

            return {
                'products': products,
                'count': len(products),
                'store': store.get('name', store_name),
                'query': query,
            }

        except Exception as e:
            logger.error(f'❌ Search error: {e}')
            return {'error': str(e), 'products': []}

    async def take_screenshot(self, filename='screenshot.png'):
        """Take a screenshot of the current page for debugging"""
        if self.page:
            path = Path(__file__).parent / filename
            await self.page.screenshot(path=str(path), full_page=True)
            logger.info(f'📸 Screenshot saved: {path}')
            return str(path)
        return None

    def get_browser_page(self):
        """Return the current page for manual interaction"""
        return self.page


# ---------------------------------------------------------------------------
# Quick-use helper functions
# ---------------------------------------------------------------------------

async def quick_scrape(store_name, max_products=10):
    """Quick function to scrape trending products from a store"""
    scraper = PerfumeScraper()
    try:
        await scraper.start_browser(headless=False)
        result = await scraper.scrape_trending(store_name, max_products=max_products)
        return result
    finally:
        # Don't close browser — keep it open for user interaction
        pass


async def quick_search(store_name, query, max_products=10):
    """Quick function to search products in a store"""
    scraper = PerfumeScraper()
    try:
        await scraper.start_browser(headless=False)
        result = await scraper.search_products(store_name, query, max_products=max_products)
        return result
    finally:
        pass


if __name__ == '__main__':
    # Configure logging for test runs
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%H:%M:%S',
    )

    async def main():
        result = await quick_scrape('khbir')
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
