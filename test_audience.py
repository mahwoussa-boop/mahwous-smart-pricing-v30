# -*- coding: utf-8 -*-
"""اختبارات منظومة جمهور حي — Audience Matrix Tests
فحص شامل لجميع الطبقات السبع
"""
import sys
import os
import json
import random

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# ═══════════════════════════════════════════════════════════
#  اختبارات الاستيراد
# ═══════════════════════════════════════════════════════════

def test_imports():
    """فحص استيراد جميع الوحدات"""
    results = {}
    
    modules = [
        'audience_db', 'audience_engine', 'review_patterns',
        'real_reviews_bank', 'personas_engine', 'review_generator',
        'anti_repeat', 'short_texts_bank', 'dialects',
    ]
    
    for mod in modules:
        try:
            __import__(mod)
            results[mod] = '✅'
        except Exception as e:
            results[mod] = f'❌ {e}'
    

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 1: أنماط التقييمات
# ═══════════════════════════════════════════════════════════

def test_review_patterns():
    """فحص أنماط التقييمات الجديدة"""
    from review_patterns import (
        REVIEW_PATTERNS, PATTERN_CATEGORIES, RATING_DISTRIBUTION,
        LOW_RATING_REASONS, pick_pattern, pick_rating,
    )
    
    issues = []
    
    # فحص وجود الفئات الجديدة
    required_categories = ['honest_critique', 'lazy', 'scent_evolution', 'store_only']
    for cat in required_categories:
        if cat not in PATTERN_CATEGORIES:
            issues.append(f'❌ فئة مفقودة: {cat}')
        else:
            count = len(PATTERN_CATEGORIES[cat])
            print(f'   ✅ فئة {cat}: {count} نمط')
    
    # فحص توزيع النجوم — الأساس المختوم: بلا سلبي (1-2) في التوزيع نفسه
    if any(r in RATING_DISTRIBUTION for r in (1, 2)):
        issues.append('❌ توزيع الأساس يحتوي سلبيًا (1-2) — يخالف المعرفة_02')
    else:
        print(f'   ✅ توزيع النجوم (بلا سلبي): {RATING_DISTRIBUTION}')
    
    # فحص أسباب التقييم المنخفض
    for rating in [1, 2, 3, 4]:
        if rating not in LOW_RATING_REASONS:
            issues.append(f'❌ أسباب مفقودة لـ {rating} نجوم')
        else:
            count = len(LOW_RATING_REASONS[rating])
            print(f'   ✅ أسباب {rating} نجوم: {count} سبب')
    
    # فحص توزيع النجوم بالتوليد
    ratings = [pick_rating() for _ in range(1000)]
    dist = {r: ratings.count(r) for r in [1, 2, 3, 4, 5]}
    print(f'   📊 توزيع 1000 تقييم: {dist}')
    
    # السلبي (1-2) محصور بالباب الخلفي الديناميكي (~1%) — سقف صارم
    neg = dist.get(1, 0) + dist.get(2, 0)
    if neg > 30:
        issues.append(f'❌ سلبي يتجاوز سقف الباب الخلفي: {neg}/1000')
    else:
        print(f'   ✅ السلبي ضمن الباب الخلفي: {neg}/1000')

    # فحص أن 5 نجوم ≈ 74%
    five_pct = dist.get(5, 0) / 10
    if not (66 <= five_pct <= 82):
        issues.append(f'❌ نسبة 5 نجوم خارج النطاق: {five_pct}%')
    else:
        print(f'   ✅ نسبة 5 نجوم: {five_pct}%')
    
    # فحص دوال المتجر
    try:
        from review_patterns import pick_store_pattern, pick_honest_pattern, is_store_pattern
        store_p = pick_store_pattern()
        if is_store_pattern(store_p):
            print(f'   ✅ pick_store_pattern: {store_p}')
        else:
            issues.append(f'❌ pick_store_pattern أرجع نمط غير متجر: {store_p}')
        
        honest_p = pick_honest_pattern()
        print(f'   ✅ pick_honest_pattern: {honest_p}')
    except ImportError as e:
        issues.append(f'❌ دوال مفقودة: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 2: بنك التقييمات الحقيقية
# ═══════════════════════════════════════════════════════════

def test_real_reviews_bank():
    """فحص بنك التقييمات الجديد"""
    issues = []
    
    try:
        from real_reviews_bank import (
            MALE_NEGATIVE, FEMALE_NEGATIVE, BALANCED_4STAR,
            STORE_REVIEWS_BANK, SCENT_FAMILY_REVIEWS,
        )
        
        banks = {
            'MALE_NEGATIVE': MALE_NEGATIVE,
            'FEMALE_NEGATIVE': FEMALE_NEGATIVE,
            'BALANCED_4STAR': BALANCED_4STAR,
            'STORE_REVIEWS_BANK': STORE_REVIEWS_BANK,
        }
        
        for name, bank in banks.items():
            count = len(bank)
            if count < 10:
                issues.append(f'❌ {name}: {count} فقط (المطلوب 10+)')
            else:
                print(f'   ✅ {name}: {count} تقييم')
        
        # فحص عائلات العطور
        required_families = ['oriental', 'fresh', 'floral', 'woody', 'gourmand', 'musk']
        for family in required_families:
            if family not in SCENT_FAMILY_REVIEWS:
                issues.append(f'❌ عائلة مفقودة: {family}')
            else:
                count = len(SCENT_FAMILY_REVIEWS[family])
                print(f'   ✅ عائلة {family}: {count} تقييم')
        
        # فحص أن تقييمات المتجر لا تذكر الريحة
        scent_words = ['ريحة', 'ريحته', 'ثبات', 'ثباته', 'فوحان', 'عود', 'مسك']
        store_violations = 0
        for review in STORE_REVIEWS_BANK:
            if any(w in review for w in scent_words):
                store_violations += 1
        
        if store_violations > 0:
            issues.append(f'⚠️ {store_violations} تقييم متجر يذكر وصف المنتج')
        else:
            print(f'   ✅ تقييمات المتجر منفصلة عن المنتج')
    
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 3: مكافحة التكرار
# ═══════════════════════════════════════════════════════════

def test_anti_repeat():
    """فحص نظام مكافحة التكرار المحدث"""
    issues = []
    
    try:
        from anti_repeat import (
            TRACKED_WORDS, 
            get_burned_words, is_duplicate,
            reset_session_texts,
        )
        
        # فحص عدد الكلمات المتتبعة
        word_count = len(TRACKED_WORDS)
        if word_count < 40:
            issues.append(f'❌ TRACKED_WORDS: {word_count} فقط (المطلوب 40+)')
        else:
            print(f'   ✅ TRACKED_WORDS: {word_count} كلمة')
        
        # فحص وجود الأنماط والسياقات الجديدة
        try:
            from anti_repeat import TRACKED_PATTERNS, TRACKED_CONTEXTS
            print(f'   ✅ TRACKED_PATTERNS: {len(TRACKED_PATTERNS)} نمط')
            print(f'   ✅ TRACKED_CONTEXTS: {len(TRACKED_CONTEXTS)} سياق')
        except ImportError:
            issues.append('❌ TRACKED_PATTERNS أو TRACKED_CONTEXTS مفقود')
        
        # فحص الدوال الجديدة
        try:
            from anti_repeat import (
                track_context, is_context_burned, get_available_contexts,
                track_pattern_structure, register_review_full,
            )
            print(f'   ✅ الدوال الجديدة موجودة')
            
            # اختبار تتبع السياقات
            reset_session_texts()
            track_context('مسجد')
            track_context('زواج')
            if is_context_burned('مسجد'):
                print(f'   ✅ تتبع السياقات يعمل')
            else:
                issues.append('❌ تتبع السياقات لا يعمل')
            
            available = get_available_contexts()
            if 'مطعم' in available:
                print(f'   ✅ السياقات المتاحة: {len(available)}')
            
        except ImportError as e:
            issues.append(f'❌ دوال مفقودة: {e}')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 4: النصوص القصيرة
# ═══════════════════════════════════════════════════════════

def test_short_texts():
    """فحص بنك النصوص القصيرة"""
    issues = []
    
    try:
        from short_texts_bank import NEGATIVE_SHORTS, NEUTRAL_SHORTS
        
        neg_count = len(NEGATIVE_SHORTS)
        neu_count = len(NEUTRAL_SHORTS)
        
        if neg_count < 15:
            issues.append(f'❌ NEGATIVE_SHORTS: {neg_count} (المطلوب 15+)')
        else:
            print(f'   ✅ NEGATIVE_SHORTS: {neg_count} نص')
        
        if neu_count < 10:
            issues.append(f'❌ NEUTRAL_SHORTS: {neu_count} (المطلوب 10+)')
        else:
            print(f'   ✅ NEUTRAL_SHORTS: {neu_count} نص')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 5: اللهجات
# ═══════════════════════════════════════════════════════════

def test_dialects():
    """فحص تعبيرات اللهجات الجديدة"""
    issues = []
    
    try:
        from dialects import NEGATIVE_DIALECT_EXPRESSIONS, COMPLAINT_PATTERNS
        
        required_dialects = ['najdi', 'hijazi', 'sharqi', 'janoubi']
        
        for dialect in required_dialects:
            if dialect not in NEGATIVE_DIALECT_EXPRESSIONS:
                issues.append(f'❌ تعبيرات سلبية مفقودة: {dialect}')
            else:
                count = len(NEGATIVE_DIALECT_EXPRESSIONS[dialect])
                print(f'   ✅ تعبيرات سلبية {dialect}: {count}')
            
            if dialect not in COMPLAINT_PATTERNS:
                issues.append(f'❌ أنماط شكوى مفقودة: {dialect}')
            else:
                count = len(COMPLAINT_PATTERNS[dialect])
                print(f'   ✅ أنماط شكوى {dialect}: {count}')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 6: الشخصيات
# ═══════════════════════════════════════════════════════════

def test_personas():
    """فحص أنماط الشخصيات الجديدة"""
    issues = []
    
    try:
        from personas_engine import ARCHETYPES
        
        # فحص وجود الأنماط الجديدة
        new_archetypes = ['كسول', 'كسولة', 'ناقد_صريح', 'ناقدة_صريحة',
                         'متردد', 'مترددة', 'متذمر_لطيف', 'متذمرة_لطيفة']
        
        existing_ids = [a['id'] for a in ARCHETYPES]
        
        for arch_id in new_archetypes:
            if arch_id in existing_ids:
                print(f'   ✅ نمط جديد: {arch_id}')
            else:
                issues.append(f'❌ نمط مفقود: {arch_id}')
        
        total = len(ARCHETYPES)
        print(f'   📊 إجمالي الأنماط: {total}')
        
        # فحص الدوال الجديدة
        try:
            from personas_engine import cross_product_variation, generate_writing_fingerprint
            
            # اختبار cross_product_variation
            test_persona = {'name': 'تجربة', 'mood': 0}
            var1 = cross_product_variation(test_persona, 0)
            var2 = cross_product_variation(test_persona, 1)
            if var1.get('_variation_style') != var2.get('_variation_style'):
                print(f'   ✅ cross_product_variation يعمل (تنوع بين المنتجات)')
            else:
                print(f'   ⚠️ cross_product_variation: نفس النتيجة لمنتجين مختلفين')
            
            # اختبار generate_writing_fingerprint
            fp = generate_writing_fingerprint(test_persona)
            if 'consistent_typos' in fp and 'preferred_emoji' in fp:
                print(f'   ✅ generate_writing_fingerprint يعمل')
            else:
                issues.append('❌ generate_writing_fingerprint ناقص')
            
        except ImportError as e:
            issues.append(f'❌ دوال مفقودة: {e}')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار الطبقة 7: قاعدة البيانات والمحرك
# ═══════════════════════════════════════════════════════════

def test_database():
    """فحص قاعدة بيانات audience_db"""
    issues = []
    
    try:
        from audience_db import AudienceDB
        
        # استخدام قاعدة بيانات مؤقتة للاختبار
        db = AudienceDB(':memory:')
        
        # فحص الجداول
        print(f'   ✅ AudienceDB تم إنشاؤها')
        
        # فحص vocabulary_bank
        can_use = db.check_vocabulary('فخم')
        print(f'   ✅ check_vocabulary يعمل: {can_use}')
        
        db.update_vocabulary(['فخم', 'رائع', 'ممتاز'])
        print(f'   ✅ update_vocabulary يعمل')
        
        # فحص persona_registry
        pid = db.register_persona('أحمد', 'male', 'متحمس', 'najdi', {'emoji': '🔥'})
        print(f'   ✅ register_persona يعمل: id={pid}')
        
        # فحص review_history
        rid = db.save_review(pid, 'عطر تجريبي', 'ممتاز والله', 'product', 5, 'ultra_short', 'متحمس', 'najdi')
        print(f'   ✅ save_review يعمل: id={rid}')
        
        # فحص الإحصائيات
        stats = db.get_full_stats()
        print(f'   ✅ get_full_stats يعمل: {len(stats)} مفتاح')
        
        db.close()
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    except Exception as e:
        issues.append(f'❌ خطأ: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

def test_engine():
    """فحص محرك جمهور حي"""
    issues = []
    
    try:
        from audience_engine import AudienceEngine
        
        engine = AudienceEngine(':memory:')
        print(f'   ✅ AudienceEngine تم إنشاؤه')
        
        # فحص توزيع النجوم
        ratings = engine._assign_ratings(100)
        dist = {r: ratings.count(r) for r in [1, 2, 3, 4, 5]}
        print(f'   📊 توزيع 100 تقييم: {dist}')
        
        five_pct = dist.get(5, 0)
        if not (50 <= five_pct <= 70):
            issues.append(f'❌ نسبة 5 نجوم خارج النطاق: {five_pct}%')
        else:
            print(f'   ✅ نسبة 5 نجوم: {five_pct}%')
        
        if dist.get(1, 0) == 0 and dist.get(2, 0) == 0:
            issues.append('❌ لا توجد تقييمات 1-2 نجوم')
        
        # فحص حقن العيوب
        flaws = engine._get_random_flaw()
        if flaws:
            print(f'   ✅ حقن العيوب يعمل: "{flaws[:40]}..."')
        
        # فحص Bell Curve
        schedule = engine.schedule_bell_curve(list(range(20)), days=7)
        if len(schedule) == 20:
            print(f'   ✅ Bell Curve: {len(schedule)} موعد')
            # فحص أن المواعيد متنوعة
            hours = [s.hour for s in schedule]
            unique_hours = len(set(hours))
            print(f'   📊 ساعات مختلفة: {unique_hours}')
        else:
            issues.append(f'❌ Bell Curve أرجع {len(schedule)} بدل 20')
        
        # فحص التحقق
        test_reviews = [
            {'text': 'ممتاز', 'rating': 5, 'type': 'product'},
            {'text': 'حلو بس ما يثبت', 'rating': 4, 'type': 'product'},
            {'text': 'التغليف مرتب', 'rating': 5, 'type': 'store'},
        ]
        validation = engine.validate_batch(test_reviews)
        print(f'   ✅ validate_batch يعمل: {len(validation)} مشكلة')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    except Exception as e:
        issues.append(f'❌ خطأ: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  اختبار التكامل: فحص التنوع في 50 تقييم
# ═══════════════════════════════════════════════════════════

def test_vocabulary_diversity():
    """فحص تنوع المفردات في بنوك التقييمات"""
    issues = []
    
    try:
        from real_reviews_bank import (
            MALE_NEGATIVE, FEMALE_NEGATIVE, BALANCED_4STAR,
            STORE_REVIEWS_BANK, SCENT_FAMILY_REVIEWS,
        )
        from review_patterns import REVIEW_PATTERNS
        
        # جمع كل التقييمات
        all_reviews = (
            MALE_NEGATIVE + FEMALE_NEGATIVE + BALANCED_4STAR + STORE_REVIEWS_BANK
        )
        for family_reviews in SCENT_FAMILY_REVIEWS.values():
            all_reviews.extend(family_reviews)
        
        total = len(all_reviews)
        print(f'   📊 إجمالي التقييمات الجديدة: {total}')
        
        # فحص تكرار الكلمات
        from anti_repeat import TRACKED_WORDS
        
        word_counts = {}
        for word in TRACKED_WORDS[:20]:  # أهم 20 كلمة
            count = sum(1 for r in all_reviews if word in r)
            pct = (count / total * 100) if total > 0 else 0
            if pct > 15:
                issues.append(f'⚠️ كلمة "{word}" تظهر في {pct:.1f}% من التقييمات')
            word_counts[word] = pct
        
        # أعلى 5 كلمات تكراراً
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for word, pct in top_words:
            if pct > 0:
                print(f'   📊 "{word}": {pct:.1f}%')
        
        # فحص عدم وجود تقييمين متطابقين
        unique_reviews = set(all_reviews)
        duplicates = total - len(unique_reviews)
        if duplicates > 0:
            issues.append(f'❌ {duplicates} تقييم متكرر!')
        else:
            print(f'   ✅ لا تقييمات متكررة ({total} فريد)')
        
        # فحص تنوع الأطوال
        lengths = [len(r.split()) for r in all_reviews]
        avg_len = sum(lengths) / len(lengths) if lengths else 0
        min_len = min(lengths) if lengths else 0
        max_len = max(lengths) if lengths else 0
        print(f'   📊 أطوال: min={min_len}, avg={avg_len:.1f}, max={max_len}')
        
    except ImportError as e:
        issues.append(f'❌ استيراد فاشل: {e}')
    
    assert not issues, '\n'.join(str(_i) for _i in issues)

# ═══════════════════════════════════════════════════════════
#  التشغيل الرئيسي
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    print('=' * 60)
    print('🧪 اختبارات منظومة جمهور حي — Audience Matrix Tests')
    print('=' * 60)
    
    all_issues = []
    
    # 1. فحص الاستيرادات
    print('\n📦 فحص الاستيرادات...')
    imports = test_imports()
    for mod, status in imports.items():
        print(f'   {status} {mod}')
    failed_imports = [m for m, s in imports.items() if '❌' in s]
    
    # 2. فحص الأنماط
    print('\n🎯 فحص أنماط التقييمات...')
    issues = test_review_patterns()
    all_issues.extend(issues)
    
    # 3. فحص بنك التقييمات
    print('\n📚 فحص بنك التقييمات...')
    issues = test_real_reviews_bank()
    all_issues.extend(issues)
    
    # 4. فحص مكافحة التكرار
    print('\n🔄 فحص مكافحة التكرار...')
    issues = test_anti_repeat()
    all_issues.extend(issues)
    
    # 5. فحص النصوص القصيرة
    print('\n📝 فحص النصوص القصيرة...')
    issues = test_short_texts()
    all_issues.extend(issues)
    
    # 6. فحص اللهجات
    print('\n🗣️ فحص اللهجات...')
    issues = test_dialects()
    all_issues.extend(issues)
    
    # 7. فحص الشخصيات
    print('\n👤 فحص الشخصيات...')
    issues = test_personas()
    all_issues.extend(issues)
    
    # 8. فحص قاعدة البيانات
    print('\n🗄️ فحص قاعدة البيانات...')
    issues = test_database()
    all_issues.extend(issues)
    
    # 9. فحص المحرك
    print('\n⚙️ فحص المحرك...')
    issues = test_engine()
    all_issues.extend(issues)
    
    # 10. فحص تنوع المفردات
    print('\n📊 فحص تنوع المفردات...')
    issues = test_vocabulary_diversity()
    all_issues.extend(issues)
    
    # ملخص
    print('\n' + '=' * 60)
    if all_issues:
        print(f'⚠️ {len(all_issues)} مشكلة:')
        for issue in all_issues:
            print(f'   {issue}')
    else:
        print('✅ جميع الاختبارات نجحت!')
    
    print(f'\n📊 الاستيرادات الفاشلة: {len(failed_imports)}')
    print(f'📊 المشاكل: {len(all_issues)}')
    print('=' * 60)
