# -*- coding: utf-8 -*-
"""نظام مكافحة التكرار المتقدم"""
import sys, os, json, time, re, threading
from pathlib import Path
from collections import OrderedDict, deque

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))
DATA_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_FILE = DATA_DIR / 'archive.json'
MAX_ARCHIVE = 500  # Updated to 500

TRACKED_WORDS = [
    # كلمات المديح المحترقة
    'فخم', 'فخمة', 'فخامة', 'يجنن', 'خرافي', 'رهيب', 'أسطوري',
    'جبار', 'دمار', 'بطل', 'خيال', 'خيالي', 'يهبل', 'هيبة',
    'روعة', 'روعه', 'واجد', 'شي ثاني', 'مو طبيعي',
    # كلمات الثبات
    'ثابت', 'ثباته', 'فواح', 'فوحان', 'يثبت', 'يفوح',
    # عبارات متكررة
    'أسرع من البرق', 'أسرع من هنقرستيشن', 'والله', 'والله العظيم',
    'يلفت الانتباه', 'شموخ', 'طول اليوم', 'مره حلو',
    # سياقات محترقة
    'شيبان المسجد', 'حارس العمارة', 'كل ما ألبسه', 'سألوني',
    'وقفني', 'يسأل', 'يسألني',
    # عبارات ختامية متكررة
    'ما راح أندم', 'أنصح فيه', 'لا يفوتكم', 'ما يطوفكم',
    'بطلب مره ثانية', 'صار المفضل', 'ما أستغني عنه',
    # وصف متكرر
    'يعبي المكان', 'يملى المكان', 'ريحة رجال', 'ريحة شيوخ',
    'ريحة ملوك', 'تحفة', 'إدمان', 'مدمن عليه',
]

# أنماط هيكلية متكررة يجب تتبعها
TRACKED_PATTERNS = [
    'كل ما ألبسه {شخص} يسألني',
    'وصلني أسرع من {شيء}',
    '{شخص} سألني وش عطرك',
    'جبته هدية ل{شخص} وفرح/ت فيه',
    'صار عطري اليومي',
    '{شخص} طلب الرابط',
    'ريحته {صفة} و{صفة}',
]

# سياقات مستخدمة يجب تبديلها
TRACKED_CONTEXTS = [
    'مسجد', 'حارس', 'عمارة', 'مصعد', 'سيارة', 'أوبر',
    'زواج', 'عزيمة', 'تخرج', 'مقابلة', 'دوام', 'جمعة',
    'نادي', 'سوق', 'مطعم', 'مقهى', 'طيارة', 'فندق',
]

_context_usage = deque(maxlen=50)
_pattern_structure_usage = deque(maxlen=30)

def _empty_archive():
    return {'reviews': [], 'store_reviews': [], 'personas': []}


class _ArchiveLock:
    """قفل حصري بين العمليات حول ملف الأرشيف — بلا اعتماديات خارجية.

    الإنتاج الفعلي يعمل بعاملَي Gunicorn (Procfile/render.yaml: --workers 2)،
    كلٌّ منهما عملية منفصلة بذاكرة session منفصلة. archive_review/archive_batch
    كانا يقرآن الملف، يعدّلانه في الذاكرة، ثم يكتبانه كاملاً — بلا قفل ولا
    كتابة ذرّية. محاكاة معزولة أثبتت: عمليتان تكتبان بالتزامن ⇒ الأرشيف
    النهائي يحتوي تقييماً واحداً بدل اثنين (كتابة مفقودة).

    O_CREAT|O_EXCL ذرّي في نظام الملفات على وندوز ولينكس كليهما.

    قفل عالق (عملية سابقة انهارت وهي تملكه) كان يعني الاستسلام الدائم:
    السلوك القديم يمضي بلا حماية بعد استنفاد المهلة *بلا حذف الملف العالق*
    (__exit__ يحذفه فقط لو امتلكناه فعلاً) — فكل استدعاء لاحق، للأبد، ينتظر
    المهلة كاملة ثم يمضي بلا قفل. أعدت إنتاج هذا: ملف .lock يدوي، عملية
    تنتظر 8 ثوانٍ فتمضي بلا حماية، والملف لا يزال موجوداً بعدها. الإصلاح:
    بعد استنفاد المهلة، نستعيد القفل بالقوة (حذف + محاولة أخيرة) بدل
    الاستسلام الدائم — القفل يُصلح نفسه تلقائياً لا يُعطَّل نهائياً.
    """
    def __init__(self, path, timeout=8.0, poll=0.05):
        self._path = f'{path}.lock'
        self._timeout = timeout
        self._poll = poll
        self._held = False

    def __enter__(self):
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self._held = True
                return self
            except FileExistsError:
                time.sleep(self._poll)
            except PermissionError:
                # وندوز: نافذة "حذف معلّق" قصيرة بعد os.remove لملف قفل سابق
                # (لا تزال مؤشّرات النظام عليه حيّة لحظياً) — تُعامَل كمحاولة
                # عادية فاشلة لا كخطأ قاتل؛ رُصدت فعلياً تحت ضغط اختباري كثيف.
                time.sleep(self._poll)
        # انتهت المهلة: قفل عالق فعلاً (عملية سابقة انهارت ولم تُحرِّره).
        # نستعيده بالقوة بدل الاستسلام الدائم — محاولة أخيرة واحدة فقط.
        try:
            os.remove(self._path)
        except OSError:
            pass
        try:
            fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            self._held = True
        except OSError:
            pass  # خسرنا سباقاً نادراً جداً مع عملية أخرى استولت للتوّ — نمضي بلا حماية هذه المرة فقط
        return self

    def __exit__(self, *exc):
        if self._held:
            try:
                os.remove(self._path)
            except OSError:
                pass


# ذاكرة قراءة الأرشيف — is_duplicate يُستدعى حتى 5 مرات لكل منتج، وكل استدعاء
# كان يعيد تحليل ملف JSON كامل (~115KB). المفتاح (mtime, size) فيبطل تلقائياً
# عند أي كتابة، ويُبطَل صراحةً في _save_archive.
_archive_cache = {'key': None, 'data': None}


def _archive_key():
    try:
        stat = ARCHIVE_FILE.stat()
        return (str(ARCHIVE_FILE), stat.st_mtime_ns, stat.st_size)
    except OSError:
        return None


def _load_archive():
    key = _archive_key()
    if key is None:
        return _empty_archive()
    if _archive_cache['key'] == key and _archive_cache['data'] is not None:
        return _archive_cache['data']
    try:
        with open(ARCHIVE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return _empty_archive()
    if not isinstance(data, dict):
        return _empty_archive()
    data.setdefault('reviews', [])
    data.setdefault('store_reviews', [])
    data.setdefault('personas', [])
    _archive_cache['key'] = key
    _archive_cache['data'] = data
    return data


def _save_archive(archive):
    """كتابة ذرّية: ملف مؤقت ثم استبدال (os.replace) — لا ملف نصف مكتوب أبداً
    حتى لو انهارت العملية أثناء الكتابة. لا تُستدعى إلا داخل _ArchiveLock.

    اسم الملف المؤقت يضمّ معرّف الخيط (threading.get_ident) لا PID وحده —
    عدّة خيوط بنفس العملية (كاختبارات التزامن) تتشارك PID، فكان يمكن أن
    يتصادم اسم الملف المؤقت بينها لو حدث تراكب توقيت.
    """
    if len(archive.get('reviews', [])) > MAX_ARCHIVE:
        archive['reviews'] = archive['reviews'][-MAX_ARCHIVE:]
    tmp_path = f'{ARCHIVE_FILE}.tmp.{os.getpid()}.{threading.get_ident()}'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(archive, f, ensure_ascii=False, indent=1)
    # وندوز: os.replace قد يفشل بـPermissionError عابرة (فحص مضاد فيروسات/
    # فهرسة على الملف الوجهة) — إعادة محاولة قصيرة تمتص الحالة العابرة.
    for _attempt in range(5):
        try:
            os.replace(tmp_path, ARCHIVE_FILE)
            break
        except PermissionError:
            if _attempt == 4:
                raise
            time.sleep(0.05)
    _archive_cache['key'] = _archive_key()
    _archive_cache['data'] = archive

def get_used_texts(limit=40):
    arc = _load_archive()
    texts = [r.get('text','') for r in arc.get('reviews',[])]
    return texts[-limit:] if len(texts) > limit else texts

def archive_review(review_text, product_name, persona_name, threshold=0.35, is_store_review=False):
    """يحفظ تقييماً — فحص التكرار والحفظ عملية ذرّية واحدة تحت القفل نفسه.

    كان الفحص (is_duplicate) يقع خارج أي قفل، والحفظ لاحقاً تحت قفل منفصل —
    فعاملا Gunicorn يمكن أن يفحصا النص نفسه معاً (كلاهما «غير مكرر» لأن لا
    أحد كتب بعد)، ثم يحفظانه بالتتابع تحت القفل: القفل يمنع فقدان الكتابة
    لا تكرار المحتوى. الفحص الآن يقع *داخل* القفل على أحدث حالة فعلية
    للأرشيف، فلا ينجو تكرار ولد من تسابق فحصين متزامنين.

    التسجيل (register_text) يقع بعد نجاح الكتابة (كالسابق) كشبكة أمان
    للمستدعين الذين لا يسجّلون بأنفسهم أثناء التوليد — آمن حتى لو استُدعي
    register_text على النص نفسه مسبقاً، لأنه صار idempotent (راجع تعليقه).

    يرجع True إن حُفظ فعلاً، False إن اكتُشف مكرراً في الفحص الأخير تحت
    الحماية فلم يُحفظ (لا يفشل الاستدعاء — المستدعون القدامى الذين يتجاهلون
    القيمة المُرجَعة يبقون آمنين: تكرار مكتشف يعني ببساطة عدم حفظ مضاعف).
    """
    entry = {
        'text': review_text,
        'product': product_name,
        'persona': persona_name,
        'ts': int(time.time())
    }
    with _ArchiveLock(ARCHIVE_FILE):
        if is_duplicate(review_text, threshold=threshold, is_store_review=is_store_review,
                        against_archive_only=True):
            return False
        arc = _load_archive()
        arc['reviews'].append(entry)
        _save_archive(arc)
    register_text(review_text, persona_name)
    return True

def archive_batch(reviews, persona_name, threshold=0.35):
    """يحفظ دفعة تقييمات — نفس ضمان archive_review الذرّي، بقفل واحد للدفعة
    كاملة لا قفل لكل عنصر. يرجع قائمة bool بترتيب reviews (True = حُفظ).

    الفحص يقع مقابل نصوص الأرشيف *بعد* دمج ما أُضيف للتوّ من هذه الدفعة
    نفسها — لا مقابل قراءة ملف ثابتة طوال الحلقة. كان is_duplicate(...,
    against_archive_only=True) يستدعي get_used_texts (قراءة ملف) في كل
    تكرار، والملف لا يُكتب إلا بعد انتهاء الحلقة كاملة (_save_archive
    مؤجَّلة)، فعنصر لاحق في نفس الدفعة لا يرى عنصراً سابقاً أُضيف للتوّ.
    الأثر الأوضح: أول دفعة على أرشيف غير موجود (لا مفتاح كاش لإبطاله)،
    حيث كل قراءة تُرجع قاموساً فارغاً جديداً — نص مكرر حرفياً مرتين في
    الدفعة نفسها كان يُقبَل مرتين معاً.
    """
    results = []
    saved_texts = []
    with _ArchiveLock(ARCHIVE_FILE):
        arc = _load_archive()
        prior_texts = [r.get('text', '') for r in arc['reviews'][-MAX_ARCHIVE:]]
        changed = False
        for rv in reviews:
            text = rv.get('text', '')
            norm = _normalize(text)
            if not norm or _matches_any_normalized(norm, prior_texts, threshold):
                results.append(False)
                continue
            arc['reviews'].append({
                'text': text,
                'product': rv.get('product', ''),
                'persona': persona_name,
                'ts': int(time.time())
            })
            prior_texts.append(text)  # يراه فحص العنصر التالي في الدفعة نفسها
            changed = True
            saved_texts.append(text)
            results.append(True)
        if changed:
            _save_archive(arc)
    for text in saved_texts:
        register_text(text, persona_name)
    return results

def clear_archive():
    with _ArchiveLock(ARCHIVE_FILE):
        _save_archive({'reviews':[], 'store_reviews':[], 'personas':[]})

def get_archive_stats():
    arc = _load_archive()
    return {
        'total_reviews': len(arc.get('reviews',[])),
        'max_capacity': MAX_ARCHIVE,
        'last_10': [{'text':r['text'],'product':r.get('product',''),'persona':r.get('persona','')}
                    for r in arc.get('reviews',[])[-10:]]
    }

# تشكيل + تطويل: زخرفة إملائية لا تغيّر الكلمة
_DIACRITICS_RE = re.compile(r'[ً-ْٰـ]')
# توحيد صور الحرف الواحد — بدونه «ريحتة» و«ريحته» نصّان مختلفان تماماً، وهو ما
# جعل الأرشيف يحتفظ بـ«ريحتة حلوة وثابتة» و«ريحته حلوة وثابتة» كمدخلتين منفصلتين.
_FOLD_MAP = str.maketrans({
    'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ٱ': 'ا',
    'ة': 'ه',
    'ى': 'ي', 'ئ': 'ي',
    'ؤ': 'و',
})

# طيّ تمطيط الحرف (رهييييب ← رهيب) — نفس المصدر المستخدم في review_generator.py
# (راجع _fold_elongation هناك)، هنا في المصدر الموحّد كي يستفيد منه مسار
# الأرشيف الحيّ (app.py/streamlit_app.py) لا الأداة الإحصائية offline وحدها.
# بلا هذا: «العطر رهيب جدا وثابت» و«العطر رهييييب جدا وثابت» — الفارق الوحيد
# تمطيط كلمة واحدة — نصّان «مختلفان» عند is_duplicate.
_ELONGATION_RE = re.compile(r'([ء-ي])\1{1,}')


def _normalize(text):
    """تطبيع إملائي عربي: يُسقط غير العربي والتشكيل، يطوي تمطيط الحرف،
    ويوحّد صور الحروف."""
    if not text:
        return ''
    t = re.sub(r'[^؀-ۿ\s]', ' ', text)
    t = _DIACRITICS_RE.sub('', t)
    t = _ELONGATION_RE.sub(r'\1', t)
    t = t.translate(_FOLD_MAP)
    return re.sub(r'\s+', ' ', t).strip()


# \u0625\u0633\u0642\u0627\u0637 \u0628\u0627\u062F\u0626\u062A\u064E\u064A \u00AB\u0648\u00BB \u0627\u0644\u0639\u0637\u0641 \u0648\u00AB\u0627\u0644\u00BB \u0627\u0644\u062A\u0639\u0631\u064A\u0641 \u2014 \u0644\u0644\u0645\u0642\u0627\u0631\u0646\u0629 \u0627\u0644\u0636\u0628\u0627\u0628\u064A\u0629 (jaccard/bigram)
# \u0641\u0642\u0637\u060C \u0644\u0627 \u0627\u0644\u062A\u0637\u0628\u064A\u0639 \u0627\u0644\u0623\u0633\u0627\u0633\u064A \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645 \u0644\u0645\u0641\u062A\u0627\u062D \u0627\u0644\u062A\u062E\u0632\u064A\u0646/\u0627\u0644\u062A\u0637\u0627\u0628\u0642 \u0627\u0644\u062D\u0631\u0641\u064A\u060C \u062A\u0641\u0627\u062F\u064A\u0627\u064B
# \u0644\u062A\u0635\u0627\u062F\u0645 \u0643\u0644\u0645\u0627\u062A \u062C\u0630\u0631\u0647\u0627 \u0627\u0644\u062D\u0642\u064A\u0642\u064A \u064A\u0628\u062F\u0623 \u0628\u0648 (\u0645\u062B\u0644 \u00AB\u0648\u0631\u062F\u0629\u00BB) \u0641\u064A \u0633\u064A\u0627\u0642 \u0627\u0644\u062A\u0637\u0627\u0628\u0642 \u0627\u0644\u0635\u0627\u0631\u0645.
# \u0647\u0646\u0627 (\u0639\u062A\u0628\u0629 \u062A\u0634\u0627\u0628\u0647 \u0644\u0627 \u062A\u0637\u0627\u0628\u0642 \u062A\u0627\u0645) \u0623\u064A \u0625\u0633\u0642\u0627\u0637 \u0646\u0627\u062F\u0631 \u062E\u0627\u0637\u0626 \u0644\u0627 \u064A\u0642\u0644\u0628 \u062D\u0643\u0645 \u0627\u0644\u062A\u0634\u0627\u0628\u0647 \u0627\u0644\u0639\u0627\u0645.
# \u0628\u0644\u0627 \u0647\u0630\u0627: \u00AB\u0627\u0644\u0639\u0637\u0631 \u062C\u0645\u064A\u0644 \u062C\u062F\u0627 \u0648\u062B\u0627\u0628\u062A\u00BB \u0648\u00AB\u0639\u0637\u0631 \u062C\u0645\u064A\u0644 \u062C\u062F\u0627 \u062B\u0627\u0628\u062A\u00BB \u064A\u064F\u062D\u0633\u0628\u0627\u0646 \u0645\u062E\u062A\u0644\u0641\u064A\u0646
# \u0631\u063A\u0645 \u0623\u0646 \u0627\u0644\u0641\u0627\u0631\u0642 \u0627\u0644\u0648\u062D\u064A\u062F \u0628\u0627\u062F\u0626\u062A\u0627\u0646 (\u0627\u0644/\u0648).
def _strip_common_prefixes(token):
    if len(token) > 4 and token.startswith('\u0648'):
        token = token[1:]
    if len(token) > 4 and token.startswith('\u0627\u0644'):
        token = token[2:]
    return token


def _tokenize_arabic(text):
    """\u0645\u062C\u0645\u0648\u0639\u0629 \u0643\u0644\u0645\u0627\u062A \u0627\u0644\u0646\u0635 \u0628\u0639\u062F \u0627\u0644\u062A\u0637\u0628\u064A\u0639 + \u0625\u0633\u0642\u0627\u0637 \u0628\u0627\u062F\u0626\u062A\u064E\u064A \u0648/\u0627\u0644 (\u0644\u0644\u0645\u0642\u0627\u0631\u0646\u0629 \u0627\u0644\u0636\u0628\u0627\u0628\u064A\u0629)."""
    return {_strip_common_prefixes(w) for w in _normalize(text).split()}

def _get_bigrams(text):
    tokens = [_strip_common_prefixes(w) for w in _normalize(text).split()]
    return set(zip(tokens, tokens[1:]))

def _jaccard_similarity(text1, text2):
    tokens1 = _tokenize_arabic(text1)
    tokens2 = _tokenize_arabic(text2)
    if not tokens1 or not tokens2:
        return 0.0
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    return len(intersection) / len(union) if union else 0.0

def _bigram_overlap(text1, text2):
    b1 = _get_bigrams(text1)
    b2 = _get_bigrams(text2)
    return len(b1 & b2)

_session_norm = OrderedDict()
_session_recent = deque(maxlen=200)
_SESSION_CAP = 8000

# Burned Words tracking
_word_usage_history = deque(maxlen=20)
_persona_keywords = {}

def register_text(text, persona_name=None):
    """يسجّل نصاً في ذاكرة الجلسة (تتبّع الاستخدام/الحرق) — **مؤمَّن ضد
    التسجيل المزدوج**: نفس التقييم الحقيقي كان يُسجَّل مرتين فعلياً (مرة
    عند التوليد عبر _register/ar_register_text، ومرة أخرى داخل
    archive_review/archive_batch عند الحفظ) — فتحرق الكلمات والبدايات
    بضعف السرعة الحقيقية (بداية واحدة بعد تقييم واحد فقط كانت تُصبح
    «محروقة» رغم أن عتبة الحرق تفترض استخدامَين حقيقيَّين مختلفَين).

    بدل تتبّع كل موضع استدعاء لإزالة الاستدعاء «الزائد» (هش، يعتمد على تذكّر
    كل مسار مستقبلي)، الدالة نفسها idempotent: نص طُبِّع مسبقاً وسُجِّل هذه
    الجلسة لا يُعاد عدّه في أي عدّاد حرق — يُلمَس فقط ترتيب LRU في
    _session_norm (غير مؤثّر على الحرق).
    """
    n = _normalize(text)
    if not n:
        return
    if n in _session_norm:
        _session_norm.move_to_end(n)
        return
    _session_norm[n] = True
    _session_norm.move_to_end(n)
    while len(_session_norm) > _SESSION_CAP:
        _session_norm.popitem(last=False)
    _session_recent.append(text)

    # المسار الحيّ (app/streamlit) يستدعي register_text وحدها، فكل تتبّع يجب أن
    # يعيش هنا. كان تتبّع السياق والفكرة داخل register_review_full التي لا
    # يستدعيها إلا سكربت demo — فبقي _context_usage فارغاً في الإنتاج، وكانت
    # get_available_contexts ترجع كل السياقات دائماً كأن شيئاً لم يُستخدم.
    track_opening(text)          # البدايات
    ctx = extract_context_from_review(text)
    if ctx:
        track_context(ctx)       # السياقات (مسجد/دوام/سيارة…)
    track_pattern_structure(extract_pattern_structure(text))  # الأفكار

    # Track words for burnout
    _word_usage_history.append(text)
    
    # Track persona fingerprint
    if persona_name:
        if persona_name not in _persona_keywords:
            _persona_keywords[persona_name] = set()
        for w in TRACKED_WORDS:
            if w in text:
                _persona_keywords[persona_name].add(w)

def is_registered(text):
    return _normalize(text) in _session_norm

def reset_session_texts():
    _session_norm.clear()
    _session_recent.clear()
    _word_usage_history.clear()
    _persona_keywords.clear()
    _opening_usage.clear()
    _context_usage.clear()
    _pattern_structure_usage.clear()

# ── عتبات حسب الطول ──────────────────────────────────────────────────────
# التشابه النسبي بلا معنى على النص القصير: كلمة مشتركة واحدة من كلمتين =
# jaccard 0.5، فعتبة 0.35 ترفض كل تقييم قصير تقريباً. و43% من len_target
# المعاير من بيانات المنافسين هو 1–2 كلمة، فكان الفرع القديم (<3 كلمات ⇒ مكرر)
# يستنفد كل المحاولات ويقبل المكرر حتماً في نحو نصف التوليدات.
SHORT_MAX_WORDS = 3


def _thresholds_for(n_words, base_threshold):
    """(عتبة jaccard، أدنى تداخل bigram) حسب طول النص."""
    if n_words <= 6:
        return 0.65, 3
    if n_words <= 11:
        return 0.50, 3
    return base_threshold, 3


def _matches_any_normalized(norm, prior_texts, threshold=0.35):
    """يقارن نصاً مُطبَّعاً مقابل قائمة نصوص خام صريحة — جوهر is_duplicate
    بلا أي اعتماد على مصدر القائمة (ملف/جلسة/غير ذلك). مستخرَجة كي تستطيع
    archive_batch فحص دفعتها الداخلية (عناصر أُضيفت للتوّ في نفس الحلقة
    ولم تُكتب للملف بعد) بنفس منطق المطابقة تماماً، لا نسخة مبسّطة تتباعد.
    """
    if not norm:
        return True
    n_words = len(norm.split())
    if n_words <= SHORT_MAX_WORDS:
        # مطابقة بمجموعة الكلمات المرتّبة (بعد إسقاط بادئتَي و/ال لكل كلمة)
        # لا سلسلة حرفية — «فخم وثابت وفواح» و«فواح وثابت فخم» نفس المعنى
        # بترتيب مختلف («وفواح» بعطف القائمة = «فواح» بلا عطف). النص القصير
        # جداً (≤3 كلمات) لا يحمل عادةً بنية نحوية يغيّر ترتيبها المعنى.
        key = tuple(sorted(_strip_common_prefixes(w) for w in norm.split()))
        return any(
            tuple(sorted(_strip_common_prefixes(w) for w in _normalize(old).split())) == key
            for old in prior_texts)
    thr, min_bigrams = _thresholds_for(n_words, threshold)
    for old in prior_texts:
        on = _normalize(old)
        if not on:
            continue
        if on == norm:
            return True
        if _jaccard_similarity(norm, on) > thr:
            return True
        if _bigram_overlap(norm, on) >= min_bigrams:
            return True
    return False


def is_duplicate(new_text, threshold=0.35, is_store_review=False, against_archive_only=False):
    """هل النص مكرر مقابل الأرشيف + الجلسة؟

    يجب أن يُستدعى على **النص النهائي** (بعد التطبيع والأنسنة والقصّ) لا على
    مخرج الـAI الخام؛ القصّ إلى بضع كلمات يجعل مخرجات مختلفة تنهار لنفس النص.

    against_archive_only: يتجاهل ذاكرة الجلسة (_session_norm/_session_recent)
    ويفحص ملف الأرشيف فقط. تستخدمه archive_review/archive_batch كفحص أخير
    تحت القفل قبل الكتابة — النص الذي وصل لهنا سُجِّل في الجلسة أثناء
    التوليد للتوّ (لمنع تكرار داخل الدفعة نفسها)، فمقارنته بذاكرة جلسته
    الخاصة تُعطي «مكرر» دائماً وتُبطل الفحص. الهدف الحقيقي هنا اكتشاف عملية
    *أخرى* كتبت النص نفسه للتوّ — وهذا لا يظهر إلا في ملف الأرشيف المشترك.
    """
    if not new_text or not new_text.strip():
        return True

    norm = _normalize(new_text)
    if not norm:
        return True

    # تطابق حرفي بعد التطبيع: مكرر مهما كان الطول
    if not against_archive_only and norm in _session_norm:
        return True

    # MAX_ARCHIVE لا رقماً ثابتاً أصغر: كان limit=100 يحجب 400 مدخلة من كل
    # أرشيف يتجاوز 100 تقييم (والأرشيف يحتفظ حتى MAX_ARCHIVE=500) — نص
    # مكرر حرفياً في المدخلة رقم 101 وما قبلها كان يمرّ كـ«غير مكرر».
    prior = list(get_used_texts(limit=MAX_ARCHIVE))
    if not against_archive_only:
        prior += list(_session_recent)
    return _matches_any_normalized(norm, prior, threshold)

def get_burned_words():
    """الكلمات المحروقة التي لا يجب استخدامها الآن"""
    burned = []
    text_history = " ".join(_word_usage_history)
    for w in TRACKED_WORDS:
        if text_history.count(w) >= 3:
            burned.append(w)
    return burned

def get_persona_fingerprint(persona_name):
    """الكلمات التي استخدمها الشخص سابقاً"""
    return list(_persona_keywords.get(persona_name, set()))

# --- Pattern Tracking ---
_pattern_counts = {}

def track_pattern(pattern_name):
    global _pattern_counts
    _pattern_counts[pattern_name] = _pattern_counts.get(pattern_name, 0) + 1

def get_pattern_counts():
    return dict(_pattern_counts)

def reset_pattern_counts():
    global _pattern_counts
    _pattern_counts = {}

def should_cooldown(pattern_name, max_consecutive=3):
    return _pattern_counts.get(pattern_name, 0) >= max_consecutive

# --- Cooldown for repeated adjectives ---
_adjective_counts = {}

def track_adjective(adj):
    global _adjective_counts
    _adjective_counts[adj] = _adjective_counts.get(adj, 0) + 1

def needs_adjective_cooldown(adj, max_uses=3):
    return _adjective_counts.get(adj, 0) >= max_uses

def reset_adjective_counts():
    global _adjective_counts
    _adjective_counts = {}

def track_context(context_type):
    """تسجيل استخدام سياق"""
    _context_usage.append(context_type)

def is_context_burned(context_type, lookback=15):
    """هل السياق مستخدم في آخر N تقييم؟"""
    recent = list(_context_usage)[-lookback:]
    return context_type in recent

def get_available_contexts(lookback=15):
    """السياقات المتاحة التي لم تُستخدم مؤخراً"""
    recent = set(list(_context_usage)[-lookback:])
    return [c for c in TRACKED_CONTEXTS if c not in recent]

def track_pattern_structure(structure):
    """تسجيل استخدام نمط هيكلي"""
    _pattern_structure_usage.append(structure)

def is_pattern_structure_burned(structure, lookback=10):
    """هل النمط الهيكلي مستخدم مؤخراً؟"""
    recent = list(_pattern_structure_usage)[-lookback:]
    return structure in recent

def extract_context_from_review(review_text):
    """استخراج السياق من نص التقييم"""
    for ctx in TRACKED_CONTEXTS:
        if ctx in review_text:
            return ctx
    return None

def extract_pattern_structure(review_text):
    """استخراج النمط الهيكلي من التقييم"""
    structures = []
    if any(w in review_text for w in ['سألني', 'يسألني', 'سألوني']):
        structures.append('compliment_question')
    if any(w in review_text for w in ['وصلني', 'وصل']):
        structures.append('delivery_comment')
    if any(w in review_text for w in ['جبته هدية', 'هديته', 'جبتها هدية']):
        structures.append('gift_story')
    if any(w in review_text for w in ['صار المفضل', 'صار عطري', 'ما أستغني']):
        structures.append('loyalty_declaration')
    if any(w in review_text for w in ['أول ما', 'لما جربته', 'أول ما رشيته']):
        structures.append('first_impression')
    return '_'.join(structures) if structures else 'generic'

# ── تتبّع البدايات ───────────────────────────────────────────────────────
# تكرار الافتتاحية أظهر بصمة آلية حتى حين يختلف باقي النص («ريحته حلوة»،
# «ريحته تملى المكان»، «ريحته تفتح النفس» — كلها تبدأ بـ«ريحته»).
_opening_usage = deque(maxlen=40)
OPENING_WORDS = 2


def opening_of(text, words=OPENING_WORDS):
    """بصمة بداية النص: أوّل كلمتين بعد التطبيع."""
    toks = _normalize(text).split()
    return ' '.join(toks[:words]) if toks else ''


def track_opening(text):
    op = opening_of(text)
    if op:
        _opening_usage.append(op)


# بداية بكلمتين متطابقتين تكفي مرّتان لحرقها؛ الكلمة الأولى وحدها أوسع فتُمنَح
# سماحاً أكبر (ثلاث مرات) — «ريحته حلوة/ريحته تملى/ريحته تفتح» بدايات مختلفة
# بكلمتين لكن تكرار «ريحته» ثلاث مرات بصمة آلية واضحة.
OPENING_MAX_USES = 2
FIRST_WORD_MAX_USES = 3


def is_opening_burned(text, lookback=12, max_uses=OPENING_MAX_USES,
                      first_word_max=FIRST_WORD_MAX_USES):
    """هل تكرّرت هذه البداية (بكلمتين أو بالكلمة الأولى) كثيراً مؤخراً؟"""
    op = opening_of(text)
    if not op:
        return False
    recent = list(_opening_usage)[-lookback:]
    if recent.count(op) >= max_uses:
        return True
    first = op.split()[0]
    return sum(1 for o in recent if o.split()[:1] == [first]) >= first_word_max


def get_burned_openings(lookback=12, max_uses=OPENING_MAX_USES,
                        first_word_max=FIRST_WORD_MAX_USES):
    """البدايات المحروقة: عبارات الكلمتين المتكرّرة + الكلمات الأولى المتكرّرة."""
    recent = list(_opening_usage)[-lookback:]
    burned = {op for op in recent if recent.count(op) >= max_uses}
    firsts = [o.split()[0] for o in recent if o.split()]
    burned |= {w for w in firsts if firsts.count(w) >= first_word_max}
    return sorted(burned)


def reset_openings():
    _opening_usage.clear()


def register_review_full(review_text, persona_name=None):
    """تسجيل كامل للتقييم: نص + بداية + سياق + فكرة.

    صار مرادفاً لـregister_text بعد نقل كل التتبّع إليها: التتبّع كان محبوساً
    هنا بينما المسار الحيّ لا يستدعي إلا register_text. تُركت للتوافق مع
    demo_audience واختباراتها.
    """
    register_text(review_text, persona_name)

def format_used_texts_block(limit=30, persona_name=None):
    used = get_used_texts(limit)
    burned = get_burned_words()
    fingerprint = get_persona_fingerprint(persona_name) if persona_name else []
    available_contexts = get_available_contexts()
    burned_openings = get_burned_openings()
    
    block = ""
    if used:
        block += "تقييمات سابقة (لا تكرر صياغتها أبداً):\n"
        block += '\n'.join([f'- {t}' for t in used]) + '\n'
        
    if burned:
        block += f"\nكلمات محظورة لأنها استخدمت بكثرة (ممنوع استخدامها نهائياً): {', '.join(burned)}\n"
        
    if fingerprint:
        block += f"\nهذا الشخص استخدم هذه الكلمات في تقييمات سابقة له، لا تجعله يكررها: {', '.join(fingerprint)}\n"

    if burned_openings:
        block += ("\nبدايات محروقة (ممنوع أن يبدأ التقييم بأي منها — ابدأ بكلمة أخرى تماماً): "
                  f"{'، '.join(burned_openings)}\n")

    if available_contexts:
        block += f"\nسياقات متاحة للاستخدام (اختر واحد فقط إذا احتجت): {', '.join(available_contexts[:5])}\n"
        
    return block

# ═══════════════════════════════════════════════════════════
#  إعادة بناء ذاكرة الحرق من الأرشيف عند بدء كل عملية
# ═══════════════════════════════════════════════════════════
# الكلمات المحروقة (_word_usage_history) وبصمة الشخصية (_persona_keywords)
# والبدايات المحروقة (_opening_usage) والسياقات (_context_usage) وبنية
# الأفكار (_pattern_structure_usage) كلها ذاكرة عملية Python صرفة — بلا أي
# نسخة في archive.json. الإنتاج الفعلي يعمل بعاملَي Gunicorn منفصلَي الذاكرة
# (Procfile/render.yaml: --workers 2)، وكل إعادة تشغيل (نشر جديد، انهيار،
# إعادة تدوير) تصفّر هذه الذاكرة أيضاً. النتيجة: كلمة/بداية حرقها عامل أو
# تشغيل سابق تعود متاحة فوراً في عامل أو تشغيل جديد رغم أن الأرشيف نفسه —
# المصدر المشترك الوحيد بين العمليات — لا يزال يحمل الدليل على استخدامها.

def _rebuild_session_from_archive():
    """يعيد تشغيل register_text على كل نص محفوظ في الأرشيف — مرة واحدة عند
    استيراد الوحدة (بداية كل عملية) — فتبدأ كل عملية (عامل Gunicorn جديد أو
    تشغيل مُعاد) بذاكرة حرق مطابقة لآخر حالة معروفة من المصدر المشترك، بدل
    ذاكرة فارغة. لا يفشل الاستيراد لو تعذّرت القراءة (أرشيف غائب/تالف ابتدائياً)."""
    try:
        arc = _load_archive()
        for r in arc.get('reviews', []):
            register_text(r.get('text', ''), r.get('persona'))
    except Exception:
        pass


_rebuild_session_from_archive()

if __name__ == '__main__':
    print(f'✅ Anti-Repeat loaded')
    t1 = 'عطر ممتاز وريحته حلوة'
    t2 = 'عطر ريحته حلوة وممتاز'
    print(f'   Similarity test: {_jaccard_similarity(t1, t2):.2f}')
    print(f'   Is duplicate: {is_duplicate(t1)}')
