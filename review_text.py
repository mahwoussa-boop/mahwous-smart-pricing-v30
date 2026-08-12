# -*- coding: utf-8 -*-
"""المسار الموحّد للنص النهائي وبوابة التفرّد — مصدر واحد لـapp.py و streamlit_app.py.

## الخلل الذي تُصلحه هذه الوحدة

كان كلا المدخلين يفحص التفرّد على **مخرج الـAI الخام** ثم يطبّق بعده سلسلة
تحويلات (أخطاء الشخصية ← أنسنة ← إزالة ترقيم ← قصّ عند حدّ الكلمات ← إنقاذ
الذيل) ويخزّن الناتج. فالنص الذي فُحص ليس النص الذي حُفظ.

القصّ هو القاتل: مع len_target قصير (43% من التوليدات تستهدف 1–2 كلمة) تنهار
مخرجات مختلفة تماماً إلى نفس البادئة بعد القصّ. مثال حقيقي من الأرشيف —
«ريحته حلوة وثابتة» تكرّرت 151 مرة رغم أن كل محاولة مرّت ببوابة تفرّد «ناجحة»
على نصّها الخام الأطول.

القاعدة هنا: **يُفحص ما يُخزَّن، حرفياً.** finalize_review_text تُنتج النص
النهائي، وwrite_unique تفحص ذلك الناتج بالضبط قبل القبول.

سلوك مقصود: بعد استنفاد المحاولات يُقبل أفضل نص متاح حتى لو مكرراً (لا نفبرك
نصاً ولا نتوقّف) — لكنه الآن يُقبل بعد فحص حقيقي لا بسبب فحص أعمى.
"""
import re

# ── تنظيف بشري: بلا ترقيم ولا رموز ولا إيموجي ────────────────────────────
# (كان مكرّراً حرفياً في app.py و streamlit_app.py — مصدر واحد يمنع التباعد)
_PUNCT_RE = re.compile(r'[،,؛;:.…·•\-–—_\"“”\'‘’`«»()\[\]{}<>/\\|*#^~=+%&@!؟?]+')
_EMOJI_RE = re.compile(
    '[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF'
    '\U00002190-\U000021FF\U00002B00-\U00002BFF️‍•]+')


def humanize(text):
    """يزيل كل علامات الترقيم والرموز والإيموجي ويترك تدفّقاً عربياً طبيعياً."""
    if not text:
        return ''
    t = text.replace('\n', ' ').replace('\r', ' ')
    t = _EMOJI_RE.sub(' ', t)
    t = _PUNCT_RE.sub(' ', t)
    return re.sub(r'\s+', ' ', t).strip()


def finalize_review_text(text, allow_words=None, humanizer=None,
                         strip_tail=None, typo_fn=None):
    """يُنتج النص كما سيُخزَّن تماماً — دالة صرفة بلا أي استدعاء AI.

    الترتيب مقصود: أخطاء الشخصية أولاً (جزء من صوتها لا أداة تفرّد)، ثم
    الأنسنة، ثم إزالة الترقيم، ثم القصّ عند الحدّ، ثم إنقاذ الذيل المبتور.

    كل الوسائط الاختيارية تُمرَّر من المُستدعي حتى تبقى هذه الوحدة بلا
    اعتماد إجباري على humanizer/dialects/semantic_guard (تدرّج آمن عند غيابها).
    """
    if not text:
        return ''
    t = text
    if typo_fn is not None:
        try:
            t = typo_fn(t)
        except Exception:
            pass
    if humanizer is not None:
        try:
            t = humanizer(t)
        except Exception:
            pass
    t = humanize(t)
    if allow_words and allow_words > 0:
        words = t.split()
        if len(words) > allow_words:
            words = words[:allow_words]
        if strip_tail is not None:
            try:
                words = strip_tail(words)
            except Exception:
                pass
        t = ' '.join(words)
    return t.strip()


def _retry_hint(attempt, rejected):
    """تلميح إعادة المحاولة — يذكر النصوص المرفوضة نفسها لا تحذيراً عاماً.

    ذكر النص المرفوض حرفياً يمنع المحرّك من إعادة إنتاج نفس البادئة، وهو
    الفارق العملي عن التلميح العام الذي كان يُعيد النص ذاته مراراً.
    """
    base = (f'\n\n⚠️ المحاولة {attempt + 1}: النص السابق مكرر أو قريب جداً من '
            'تقييم موجود. غيّر البداية والكلمات والفكرة كلياً.')
    if rejected:
        uniq = list(dict.fromkeys(rejected))[-3:]
        base += '\nنصوص مرفوضة لا تُعدها ولا تُعد بدايتها: ' + ' · '.join(f'«{t}»' for t in uniq)
    base += '\nلا تحشُ الجملة بكلمات زائدة ولا تفتعل أخطاء إملائية للتغيير — غيّر المعنى والزاوية.'
    return base


def write_unique(call, parse, finalize, is_dup, prompt, max_tokens,
                 attempts=5, base_temp=0.9, temp_step=0.06, max_temp=1.2):
    """حلقة التوليد مع بوابة تفرّد تفحص **النص النهائي**.

    call(prompt, max_tokens, temperature) -> نص خام أو None
    parse(raw) -> dict أو None
    finalize(text) -> النص النهائي كما سيُخزَّن
    is_dup(final_text) -> bool

    يرجع (rv_dict, final_text). عند تعذّر الـAI كلياً: (None, '').
    """
    best = None
    rejected = []
    for k in range(attempts):
        hint = '' if k == 0 else _retry_hint(k, rejected)
        try:
            raw = call(prompt + hint, max_tokens, min(max_temp, base_temp + k * temp_step))
        except Exception:
            continue
        rv = parse(raw)
        if not isinstance(rv, dict):
            continue
        txt = (rv.get('text') or '').strip()
        if not txt:
            continue
        final = finalize(txt)
        if not final:
            continue
        if best is None:
            best = (rv, final)
        if not is_dup(final):
            return rv, final
        rejected.append(final)
    return best if best is not None else (None, '')
