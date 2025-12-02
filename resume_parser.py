from pdfminer.high_level import extract_text
import re
from collections import Counter

EXTENDED_STOP = {
  'and','the','to','in','of','for','with','a','an','is','on','at',
  'experience','work','summary','skills','education','references',
  'university','college','school','manager','team','led','worked',
  'responsible','creating','using','developed','years','month'
}

def extract_resume_keywords(pdf_path, top_n=7):
    text = extract_text(pdf_path)
    text = re.sub(r'[^a-zA-Z\s]', ' ', text).lower()
    words = [w for w in text.split() if w not in EXTENDED_STOP and len(w)>2]
    counter = Counter(words)
    return [kw for kw,_ in counter.most_common(top_n)]
