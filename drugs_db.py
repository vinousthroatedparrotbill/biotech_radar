"""주요 바이오 약물 → 기전 매핑 (curated).

매칭 규칙: 약물명/브랜드명을 lowercase normalize한 뒤 substring 매치.
- generic 이름은 INN 표기, 브랜드명은 등록상표 그대로
- 우선순위: 명시 매핑 > 접미사 룰 > 본문 키워드
"""
from __future__ import annotations

# 기전 라벨 — 사용자 사양에서 언급된 것 우선
MOA_LABELS = (
    "CAR-T", "TCE", "ADC", "ICI", "GLP-1", "Gene therapy", "RNAi", "ASO",
    "mRNA", "Kinase inhibitor", "PROTAC", "Radioligand", "Cell therapy",
    "CFTR modulator", "Complement", "Anti-angiogenic", "Anti-CGRP", "JAK inhibitor",
)

# generic_or_brand → MOA
DRUG_MOA: dict[str, str] = {
    # CAR-T
    "axicabtagene ciloleucel": "CAR-T", "yescarta": "CAR-T",
    "tisagenlecleucel": "CAR-T", "kymriah": "CAR-T",
    "brexucabtagene autoleucel": "CAR-T", "tecartus": "CAR-T",
    "lisocabtagene maraleucel": "CAR-T", "breyanzi": "CAR-T",
    "idecabtagene vicleucel": "CAR-T", "abecma": "CAR-T", "ide-cel": "CAR-T",
    "ciltacabtagene autoleucel": "CAR-T", "carvykti": "CAR-T", "cilta-cel": "CAR-T",
    "obecabtagene autoleucel": "CAR-T", "obe-cel": "CAR-T",
    # TCE / Bispecific
    "tarlatamab": "TCE", "imdelltra": "TCE",
    "tebentafusp": "TCE", "kimmtrak": "TCE",
    "mosunetuzumab": "TCE", "lunsumio": "TCE",
    "glofitamab": "TCE", "columvi": "TCE",
    "epcoritamab": "TCE", "epkinly": "TCE",
    "talquetamab": "TCE", "talvey": "TCE",
    "elranatamab": "TCE", "elrexfio": "TCE",
    "teclistamab": "TCE", "tecvayli": "TCE",
    "blinatumomab": "TCE", "blincyto": "TCE",
    # ADC
    "trastuzumab deruxtecan": "ADC", "enhertu": "ADC",
    "sacituzumab govitecan": "ADC", "trodelvy": "ADC",
    "polatuzumab vedotin": "ADC", "polivy": "ADC",
    "brentuximab vedotin": "ADC", "adcetris": "ADC",
    "trastuzumab emtansine": "ADC", "kadcyla": "ADC",
    "enfortumab vedotin": "ADC", "padcev": "ADC",
    "tisotumab vedotin": "ADC", "tivdak": "ADC",
    "mirvetuximab soravtansine": "ADC", "elahere": "ADC",
    "loncastuximab tesirine": "ADC", "zynlonta": "ADC",
    "datopotamab deruxtecan": "ADC", "datroway": "ADC",
    "patritumab deruxtecan": "ADC",
    # ICI (Immune Checkpoint Inhibitor)
    "pembrolizumab": "ICI", "keytruda": "ICI",
    "nivolumab": "ICI", "opdivo": "ICI",
    "atezolizumab": "ICI", "tecentriq": "ICI",
    "durvalumab": "ICI", "imfinzi": "ICI",
    "ipilimumab": "ICI", "yervoy": "ICI",
    "cemiplimab": "ICI", "libtayo": "ICI",
    "tremelimumab": "ICI", "imjudo": "ICI",
    "dostarlimab": "ICI", "jemperli": "ICI",
    "tislelizumab": "ICI",
    "retifanlimab": "ICI",
    "relatlimab": "ICI", "opdualag": "ICI",
    # GLP-1
    "semaglutide": "GLP-1", "ozempic": "GLP-1", "wegovy": "GLP-1", "rybelsus": "GLP-1",
    "tirzepatide": "GLP-1", "mounjaro": "GLP-1", "zepbound": "GLP-1",
    "liraglutide": "GLP-1", "victoza": "GLP-1", "saxenda": "GLP-1",
    "dulaglutide": "GLP-1", "trulicity": "GLP-1",
    "retatrutide": "GLP-1",
    "orforglipron": "GLP-1",
    "survodutide": "GLP-1",
    "ecnoglutide": "GLP-1",
    # Gene therapy / Cell therapy
    "casgevy": "Gene therapy",
    "exa-cel": "Gene therapy", "exagamglogene": "Gene therapy",
    "lyfgenia": "Gene therapy", "lovotibeglogene": "Gene therapy", "lovo-cel": "Gene therapy",
    "elevidys": "Gene therapy", "delandistrogene": "Gene therapy",
    "luxturna": "Gene therapy", "voretigene": "Gene therapy",
    "zolgensma": "Gene therapy", "onasemnogene": "Gene therapy",
    "skysona": "Gene therapy", "elivaldogene": "Gene therapy",
    "roctavian": "Gene therapy", "valoctocogene": "Gene therapy",
    "hemgenix": "Gene therapy", "etranacogene": "Gene therapy",
    "beqvez": "Gene therapy", "fidanacogene": "Gene therapy",
    # RNAi / ASO
    "patisiran": "RNAi", "onpattro": "RNAi",
    "vutrisiran": "RNAi", "amvuttra": "RNAi",
    "givosiran": "RNAi", "givlaari": "RNAi",
    "lumasiran": "RNAi", "oxlumo": "RNAi",
    "inclisiran": "RNAi", "leqvio": "RNAi",
    "fitusiran": "RNAi", "qfitlia": "RNAi",
    "nedosiran": "RNAi", "rivfloza": "RNAi",
    "nusinersen": "ASO", "spinraza": "ASO",
    "tofersen": "ASO", "qalsody": "ASO",
    "eteplirsen": "ASO", "exondys 51": "ASO",
    "casimersen": "ASO", "amondys 45": "ASO",
    "golodirsen": "ASO", "vyondys 53": "ASO",
    "tominersen": "ASO",
    # mRNA
    "spikevax": "mRNA", "comirnaty": "mRNA",
    "mrna-1083": "mRNA", "mrna-1010": "mRNA", "mrna-1647": "mRNA",
    "mrna-4157": "mRNA", "intismeran": "mRNA",
    # Kinase inhibitors
    "imatinib": "Kinase inhibitor", "gleevec": "Kinase inhibitor",
    "dasatinib": "Kinase inhibitor", "sprycel": "Kinase inhibitor",
    "ibrutinib": "Kinase inhibitor", "imbruvica": "Kinase inhibitor",
    "acalabrutinib": "Kinase inhibitor", "calquence": "Kinase inhibitor",
    "zanubrutinib": "Kinase inhibitor", "brukinsa": "Kinase inhibitor",
    "pirtobrutinib": "Kinase inhibitor", "jaypirca": "Kinase inhibitor",
    "osimertinib": "Kinase inhibitor", "tagrisso": "Kinase inhibitor",
    "lazertinib": "Kinase inhibitor", "lazcluze": "Kinase inhibitor",
    "lorlatinib": "Kinase inhibitor", "lorbrena": "Kinase inhibitor",
    "alectinib": "Kinase inhibitor", "alecensa": "Kinase inhibitor",
    "selpercatinib": "Kinase inhibitor", "retevmo": "Kinase inhibitor",
    "pralsetinib": "Kinase inhibitor", "gavreto": "Kinase inhibitor",
    "capmatinib": "Kinase inhibitor", "tabrecta": "Kinase inhibitor",
    "tepotinib": "Kinase inhibitor", "tepmetko": "Kinase inhibitor",
    "ruxolitinib": "JAK inhibitor", "jakafi": "JAK inhibitor",
    "tofacitinib": "JAK inhibitor", "xeljanz": "JAK inhibitor",
    "baricitinib": "JAK inhibitor", "olumiant": "JAK inhibitor",
    "upadacitinib": "JAK inhibitor", "rinvoq": "JAK inhibitor",
    "abrocitinib": "JAK inhibitor", "cibinqo": "JAK inhibitor",
    "sotorasib": "Kinase inhibitor", "lumakras": "Kinase inhibitor",
    "adagrasib": "Kinase inhibitor", "krazati": "Kinase inhibitor",
    "divarasib": "Kinase inhibitor",
    # PROTAC
    "vepdegestrant": "PROTAC",
    "arv-471": "PROTAC", "arv-110": "PROTAC", "arv-766": "PROTAC",
    # Radioligand
    "lutetium-177": "Radioligand", "pluvicto": "Radioligand", "lu-177-psma": "Radioligand",
    "vipivotide tetraxetan": "Radioligand",
    "azedra": "Radioligand", "iobenguane i-131": "Radioligand",
    "xofigo": "Radioligand", "radium-223": "Radioligand",
    "actinium-225": "Radioligand",
    # CFTR
    "kaftrio": "CFTR modulator", "trikafta": "CFTR modulator",
    "elexacaftor": "CFTR modulator", "tezacaftor": "CFTR modulator",
    "ivacaftor": "CFTR modulator", "kalydeco": "CFTR modulator",
    "lumacaftor": "CFTR modulator", "orkambi": "CFTR modulator",
    "vanzacaftor": "CFTR modulator",
    # Anti-CGRP migraine
    "aimovig": "Anti-CGRP", "erenumab": "Anti-CGRP",
    "ajovy": "Anti-CGRP", "fremanezumab": "Anti-CGRP",
    "emgality": "Anti-CGRP", "galcanezumab": "Anti-CGRP",
    "vyepti": "Anti-CGRP", "eptinezumab": "Anti-CGRP",
    "rimegepant": "Anti-CGRP", "nurtec": "Anti-CGRP",
    "ubrogepant": "Anti-CGRP", "ubrelvy": "Anti-CGRP",
    "atogepant": "Anti-CGRP", "qulipta": "Anti-CGRP",
    "zavzpret": "Anti-CGRP", "zavegepant": "Anti-CGRP",
    # Complement
    "soliris": "Complement", "eculizumab": "Complement",
    "ultomiris": "Complement", "ravulizumab": "Complement",
    "empaveli": "Complement", "pegcetacoplan": "Complement",
    "syfovre": "Complement",
    "iptacopan": "Complement", "fabhalta": "Complement",
    # Anti-angiogenic
    "bevacizumab": "Anti-angiogenic", "avastin": "Anti-angiogenic",
    "ramucirumab": "Anti-angiogenic", "cyramza": "Anti-angiogenic",
    "aflibercept": "Anti-angiogenic", "eylea": "Anti-angiogenic",
    "ranibizumab": "Anti-angiogenic", "lucentis": "Anti-angiogenic",
    "faricimab": "Anti-angiogenic", "vabysmo": "Anti-angiogenic",
}


# 접미사 → MOA 추정 (명시 매핑 없을 때만 사용)
SUFFIX_RULES: list[tuple[str, str]] = [
    # CAR-T 식별 접미사
    ("cabtagene", "CAR-T"),
    # ADC payload 접미사
    ("vedotin", "ADC"),
    ("deruxtecan", "ADC"),
    ("govitecan", "ADC"),
    ("tesirine", "ADC"),
    ("emtansine", "ADC"),
    ("soravtansine", "ADC"),
    # 기타
    ("siran", "RNAi"),
    ("rsen", "ASO"),
    ("ersen", "ASO"),
    ("glipron", "GLP-1"),
    ("glutide", "GLP-1"),
    # 키나제
    ("ciclib", "Kinase inhibitor"),
    ("rafenib", "Kinase inhibitor"),
    ("racib", "Kinase inhibitor"),
    ("metinib", "Kinase inhibitor"),
    ("rasib", "Kinase inhibitor"),
    ("brutinib", "Kinase inhibitor"),
    ("tinib", "Kinase inhibitor"),
    # Antibody (광범위 — 추가 구분은 컨텍스트 필요)
    ("mab", "Antibody (mAb)"),
]


# 본문 키워드 → MOA (명시/접미사로 못 찾았을 때 마지막 시도)
CONTEXT_KEYWORDS: dict[str, list[str]] = {
    "CAR-T":          ["CAR-T", "CAR T cell", "chimeric antigen receptor"],
    "TCE":            ["bispecific T-cell engager", "T-cell engager", "T cell engager", "BiTE", "bispecific antibody"],
    "ADC":            ["antibody-drug conjugate", "antibody drug conjugate", " ADC ", "(ADC)"],
    "ICI":            ["checkpoint inhibitor", "PD-1", "PD-L1", "CTLA-4", "anti-PD"],
    "GLP-1":          ["GLP-1 receptor", "GLP-1", "GLP1 agonist", "incretin"],
    "Gene therapy":   ["gene therapy", "AAV vector", "lentiviral", "in vivo gene editing"],
    "RNAi":           [" siRNA ", "RNA interference", " RNAi "],
    "ASO":            ["antisense oligonucleotide", " ASO ", "antisense therapy"],
    "mRNA":           ["mRNA vaccine", "mRNA therapeutic", "messenger RNA"],
    "Kinase inhibitor": ["kinase inhibitor", "TKI"],
    "JAK inhibitor":  ["JAK inhibitor"],
    "PROTAC":         ["PROTAC", "protein degrader", "molecular glue"],
    "Radioligand":    ["radioligand", "radiopharmaceutical", "Lu-177", "actinium-225", "PSMA-targeted"],
    "Complement":     ["complement inhibitor", "C5 inhibitor", "factor B inhibitor"],
    "CFTR modulator": ["CFTR modulator", "cystic fibrosis transmembrane"],
    "Anti-CGRP":      ["CGRP", "calcitonin gene-related peptide"],
    "Anti-angiogenic": ["VEGF inhibitor", "anti-VEGF", "angiogenesis"],
}


def classify(drug: str, context: str = "") -> str | None:
    """단일 약물명에 대해 기전 추론. 우선순위: 사전 > 접미사 > 컨텍스트."""
    if not drug:
        return None
    norm = drug.lower().strip().replace("(", "").replace(")", "")
    # 1) 사전
    for k, moa in DRUG_MOA.items():
        if k in norm or norm in k:
            return moa
    # 2) 접미사
    for suf, moa in SUFFIX_RULES:
        if norm.endswith(suf):
            return moa
    # 3) 컨텍스트 (본문에 키워드)
    if context:
        ctx = context.lower()
        for moa, kws in CONTEXT_KEYWORDS.items():
            for kw in kws:
                if kw.lower() in ctx:
                    return moa
    return None
