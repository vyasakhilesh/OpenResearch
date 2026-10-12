import collections
import csv
import pickle
import re
import unicodedata
from pathlib import Path
 
import pycountry
from babel import Locale
from babel.localedata import locale_identifiers
 
 
def norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).casefold()
    out = []
    for ch in unicodedata.normalize("NFD", s):
        cat = unicodedata.category(ch)
        if cat == "Cf":                                   # zero-width joiners etc.
            continue
        if cat == "Mn" and out and "LATIN" in unicodedata.name(out[-1], ""):
            continue                                      # keep marks on CJK, Indic, Cyrillic...
        out.append(" " if cat[0] in "PSZC" else ch)
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", "".join(out))).strip()
 
 
class PlaceResolver:
    VALID = {c.alpha_2 for c in pycountry.countries} | {"XK"}
    EN_COUNTRY = Locale("en").territories                 # ISO alpha-2 -> English name (CLDR)
    OVERRIDES = {"uk": "GB", "burma": "MM", "swaziland": "SZ", "holland": "NL", "zaire": "CD"}
    SKIP_LANG = {"link", "post", "iata", "icao", "faac", "tcid", "lauc", "unlc",
                 "wkdt", "phon", "fr_1793", "geoid"}      # alternate-name "languages" that aren't names
 
    def __init__(self, data_dir="geonames", country_aliases=None, use_cache=True):
        self.data = Path(data_dir)
        self.overrides = dict(self.OVERRIDES)
        self.overrides.update({norm(k): v for k, v in (country_aliases or {}).items()})
        self.codes = {}
        for c in pycountry.countries:
            self.codes[c.alpha_2.lower()] = c.alpha_2
            self.codes[c.alpha_3.lower()] = c.alpha_2
        self.codes["xk"] = "XK"
        self.country_names, self.places, self.index, self.en_pref, self.admin1 = self._load(use_cache)
        self._memo = {}
 
    # ------------------------------------------------------------ public API
    def resolve(self, address):
        parts = re.split(r"[,;|\n、，]+", address) if isinstance(address, str) else address
        tokens = tuple(t.strip() for t in parts if t and t.strip())
        if tokens not in self._memo:
            self._memo[tokens] = self._resolve_tokens(tokens)
        return dict(self._memo[tokens])
 
    def resolve_many(self, addresses):
        return [self.resolve(a) for a in addresses]
 
    # ------------------------------------------------------------ resolution
    def _resolve_tokens(self, tokens):
        best = max(self._readings(tokens), key=lambda r: r[0], default=None)
        if best is None:
            return {"city": None, "state": None, "country": None}
        _, cc, city, state = best
        return {"city": self._english(city) if city else None,
                "state": self._english(state) if state else None,
                "country": self.EN_COUNTRY.get(cc)}
 
    def _english(self, gid):
        return self.en_pref.get(gid) or self.places[gid]["name"]
 
    def _country_candidates(self, token):
        key = norm(token)
        compact = key.replace(" ", "")                    # "u s a" -> "usa"
        out = {}
        for k in (key, compact):
            if k in self.overrides:
                out[self.overrides[k]] = 2
        if key in self.country_names:
            out[self.country_names[key]] = 2
        if len(compact) in (2, 3) and compact in self.codes:
            out.setdefault(self.codes[compact], 1)
        return out
 
    def _readings(self, tokens):
        # score = (parts explained, country-match strength, city population); highest wins
        keys = [norm(t) for t in tokens]
        options = [(None, 0, -1)]                         # (country, strength, token it came from)
        for i, t in enumerate(tokens):
            options += [(cc, s, i) for cc, s in self._country_candidates(t).items()]
 
        for cc, strength, ci in options:
            rest = [k for i, k in enumerate(keys) if i != ci]
            base = 1 if cc else 0
 
            for i, k in enumerate(rest):                  # part i is the city
                for gid in self.index.get(("city", k), ()):
                    p = self.places[gid]
                    if cc and p["country"] != cc:
                        continue
                    st = self.admin1.get((p["country"], p["admin1"]))
                    state_named = st and any(st in self.index.get(("admin1", o), ())
                                             for j, o in enumerate(rest) if j != i)
                    yield (base + 1 + bool(state_named), strength, p["pop"]), p["country"], gid, st
 
            for k in rest:                                # no city: a part is a state
                for gid in self.index.get(("admin1", k), ()):
                    p = self.places[gid]
                    if not cc or p["country"] == cc:
                        yield (base + 1, strength, 0), p["country"], None, gid
 
            if cc:                                        # country only
                yield (1, strength, 0), cc, None, None
 
    # ------------------------------------------------------------ data loading
    def _load(self, use_cache):
        cache = self.data / "index.pkl"
        if use_cache and cache.exists():
            return pickle.loads(cache.read_bytes())
        data = (self._build_country_names(), *self._build_geonames())
        if use_cache:
            cache.write_bytes(pickle.dumps(data, protocol=5))
        return data
 
    def _build_country_names(self):
        names = {}
        for loc in sorted(locale_identifiers(), key=lambda l: not l.startswith("en")):  # English first
            try:
                terr = Locale.parse(loc).territories
            except Exception:
                continue
            for code, name in terr.items():
                if code in self.VALID:
                    names.setdefault(norm(name), code)
        for c in pycountry.countries:
            for n in (c.name, getattr(c, "official_name", None), getattr(c, "common_name", None)):
                if n:
                    names.setdefault(norm(n), c.alpha_2)
        return names
 
    @staticmethod
    def _rows(path):
        with open(path, encoding="utf-8", newline="") as f:
            yield from csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
 
    def _build_geonames(self):
        places = {}                                       
        index = collections.defaultdict(set)              
        en_pref = {}                                      
        admin1 = {}                                       
 
        for r in self._rows(self.data / "admin1CodesASCII.txt"):
            cc, code = r[0].split(".", 1)
            places[r[3]] = dict(kind="admin1", name=r[1], country=cc, admin1=code, pop=0)
            admin1[(cc, code)] = r[3]
            for n in (r[1], r[2]):
                index[("admin1", norm(n))].add(r[3])
            if cc == "US":
                index[("admin1", norm(code))].add(r[3])             # "TX" -> Texas
 
        for r in self._rows(self.data / "cities15000.txt"):
            gid, name, ascii_, alts = r[0], r[1], r[2], r[3]
            places[gid] = dict(kind="city", name=name, country=r[8], admin1=r[10], pop=int(r[14] or 0))
            for n in {name, ascii_, *alts.split(",")}:
                if n:
                    index[("city", norm(n))].add(gid)
 
        alt = self.data / "alternateNamesV2.txt"          
        if alt.exists():
            for r in self._rows(alt):                     
                gid, lang, name = r[1], r[2], r[3]
                p = places.get(gid)
                if p is None or lang in self.SKIP_LANG:
                    continue
                index[(p["kind"], norm(name))].add(gid)
                if lang == "en" and len(r) > 4 and r[4] == "1":
                    en_pref[gid] = name
        return places, index, en_pref, admin1
 
 
if __name__ == "__main__":
    resolver = PlaceResolver("./data/geonames")
    for addr in ["Jaipur, In", "In, Jaipur", ["日本", "東京"]]:
        print(addr, "->", resolver.resolve(addr))