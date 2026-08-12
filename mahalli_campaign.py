# -*- coding: utf-8 -*-
"""حملة "محلي" العبقرية لتصدر ماركة كربتك بجانب الترند العالمي"""

import sys, os, json, random, re, time
from pathlib import Path

# Fix encoding
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except:
    pass

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))

# _archive_batch كان مستورداً بلا أي استخدام (تحقّق شامل: لا استدعاء، ولا
# getattr/importlib/__all__، ولا وحدة تستورد هذه الوحدة) — الأرشفة تقع داخل
# _ai_reviews نفسها. أُزيل الاستيراد الميت.
from app import _ai_reviews, PRODUCTS
from personas_engine import generate_persona


def _dedupe_by_display_name(products):
    """يُسقط المنتجات المتطابقة بالاسم المعروض (بعد تطبيع المسافات).

    هذا السكربت يبني السلة يدوياً (شرائح من مجمعين) ولا يمرّ بـ
    trending.smart_blend، فلا تشمله حماية الاسم المعروض المضافة هناك. والكتالوج
    يحوي فعلاً أزواجاً لا تختلف إلا بمسافة مضاعفة يعرضها المتصفح متطابقة —
    فيمكن أن تدخل نسختان من العطر نفسه سلة واحدة ويُولَّد لكلٍّ منهما تقييم
    مستقل يُكتب في الأرشيف الحيّ.
    """
    seen, out = set(), []
    for p in products:
        key = re.sub(r'\s+', ' ', (p.get('name') or '')).strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(p)
    return out

# 1. تحديد ماركات وعطور الترند العالمي (الأكثر بحثاً ومبيعاً)
GLOBAL_TRENDS_KEYWORDS = [
    'ديور', 'dior', 'شانيل', 'chanel', 'توم فورد', 'tom ford', 
    'ايف سان لوران', 'ysl', 'لطافة', 'lattafa', 'فيرساتشي', 'versace',
    'ارماني', 'armani', 'كريد', 'creed', 'عساف', 'assaf', 'سوفاج', 'sauvage',
    'ليبر', 'libre', 'باكارات', 'baccarat', 'لانكوم', 'lancome', 'جيفنشي', 'givenchy',
    'كارولينا هيريرا', 'carolina', 'فالنتينو', 'valentino', 'مونت بلانك', 'mont blanc'
]

def get_mahalli_pools():
    """تحضير سلتين: منتجات كربتك، وأفضل 100 عطر ترند"""
    cryptic_pool = []
    trend_pool = []
    
    for p in PRODUCTS:
        name = p.get('name', '').lower()
        brand = p.get('brand', '').lower()
        
        # تجميع كربتك
        if 'كربتك' in name or 'كربتك' in brand or 'cryptic' in name or 'cryptic' in brand:
            cryptic_pool.append(p)
            continue
            
        # تجميع الترند
        if any(kw in name or kw in brand for kw in GLOBAL_TRENDS_KEYWORDS):
            if p.get('product_type') == 'عطر': # التركيز على العطور للترند
                trend_pool.append(p)
                
    # نأخذ أفضل 100 عطر ترند
    random.shuffle(trend_pool)
    top_100_trend = trend_pool[:100]
    
    return cryptic_pool, top_100_trend

def run_daily_campaign():
    """تشغيل حملة اليوم الواحد (15 عميل)"""
    print("🚀 بدء حملة 'محلي' العبقرية لتصدر كربتك...")
    cryptic_pool, trend_pool = get_mahalli_pools()
    
    print(f"📦 تم العثور على {len(cryptic_pool)} منتج لكربتك.")
    print(f"🌟 تم تجهيز {len(trend_pool)} عطر ترند عالمي.")
    
    # 15 عميل في اليوم
    daily_personas_count = 15
    total_reviews_generated = 0
    
    for i in range(daily_personas_count):
        print(f"\n👤 [العميل {i+1}/{daily_personas_count}] بناء شخصية جديدة...")
        persona = generate_persona()
        
        # نجلب معلومات الأركيتايب (الجنس والمفضلات)
        from app import ARCHETYPES
        arch = next((a for a in ARCHETYPES if a['id'] == persona.get('archId')), None)
        if not arch: continue
        
        # تحديد عدد منتجات السلة لهذا العميل (من 5 إلى 15)
        basket_size = random.randint(5, 15)
        
        # 1. سحب كربتك (حتى 6 منتجات، أو كل المتاح)
        is_gift_male = persona.get('archId') == 'هدايا_رجل'
        is_gift_female = persona.get('archId') == 'هدايا_أنثى'
        
        valid_cryptic = []
        for p in cryptic_pool:
            ptype = p.get('product_type', 'عطر')
            # الإناث لا يقيمن رجالي (إلا هدايا)
            if arch['g'] == 'female' and not is_gift_female and p['g'] == 'رجالي':
                continue
            # الذكور لا يقيمون مكياج (إلا هدايا)
            if arch['g'] == 'male' and not is_gift_male and ptype in ['مكياج', 'معطر_شعر']:
                continue
            valid_cryptic.append(p)
            
        # نأخذ المتاح من كربتك بحد أقصى 6
        random.shuffle(valid_cryptic)
        selected_cryptic = valid_cryptic[:6]
        
        # 2. إكمال السلة من الترند العالمي لتصل للحجم المطلوب
        remaining_slots = basket_size - len(selected_cryptic)
        if remaining_slots < 0: remaining_slots = 0
        
        valid_trend = []
        for p in trend_pool:
            if p['g'] not in arch['prefers']:
                continue
            # الفلترة الجنسية
            ptype = p.get('product_type', 'عطر')
            if arch['g'] == 'female' and not is_gift_female and p['g'] == 'رجالي':
                continue
            if arch['g'] == 'male' and not is_gift_male and ptype in ['مكياج', 'معطر_شعر']:
                continue
            valid_trend.append(p)
            
        random.shuffle(valid_trend)
        selected_trend = valid_trend[:remaining_slots]
        
        # دمج السلة — مع إسقاط ما يتطابق بالاسم المعروض (نسختان لعطر واحد
        # في الكتالوج لا تختلفان إلا بمسافة) قبل توليد أي تقييم لهما.
        final_basket = _dedupe_by_display_name(selected_cryptic + selected_trend)
        random.shuffle(final_basket) # خلط المنتجات في السلة
        
        print(f"🛒 سلة العميل {persona['name']}: {len(final_basket)} منتجات ({len(selected_cryptic)} كربتك، {len(selected_trend)} ترند).")
        
        # 3. توليد التقييمات للعميل دفعة واحدة
        if final_basket:
            try:
                reviews = _ai_reviews(persona, final_basket)
                total_reviews_generated += len(reviews)
                print(f"✅ تم توليد {len(reviews)} تقييم بنجاح للعميل {persona['name']}.")
            except Exception as e:
                print(f"❌ حدث خطأ أثناء التوليد للعميل {persona['name']}: {e}")
                
        # انتظار بسيط لتجنب Rate Limits
        time.sleep(2)
        
    print(f"\n🎉 انتهت الحملة اليومية. إجمالي التقييمات المولدة: {total_reviews_generated}")

if __name__ == '__main__':
    run_daily_campaign()
