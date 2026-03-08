import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, UTC
import re, csv, time, random, os, datetime, subprocess, requests, glob

# ============================================================
# PARAMÈTRES DES INVESTISSEURS (modernes)
# ============================================================

INVESTORS = {
    "TWEBX": "Tweedy Browne Co. - Tweedy Browne Value Fund",
    "DODGX": "Dodge & Cox",
    "MPGFX": "Mairs & Power Growth Fund",
    "MAVFX": "David Katz - Matrix Asset Advisors",
    "FPPTX": "FPA Queens Road Small Cap Value Fund",
    "FPACX": "Steven Romick - FPA Crescent Fund",
    "hcmax": "Hillman Value Fund",
    "oaklx": "Bill Nygren - Oakmark Select Fund",
    "LLPFX": "Mason Hawkins - Longleaf Partners",
    "SEQUX": "Ruane Cunniff - Sequoia Fund",
    "MVALX": "Meridian Contrarian Fund",
    "CAAPX": "John Rogers - Ariel Appreciation Fund",
    "ARFFX": "Charles Bobrinskoy - Ariel Focus Fund",
    "SA": "Christopher Bloomstran - Semper Augustus",
    "AM": "David Tepper - Appaloosa Management",
    "MP": "Tom Bancroft - Makaira Partners",
    "mc": "Lee Ainslie - Maverick Capital",
    "fairx": "Bruce Berkowitz - Fairholme Capital",
    "TF": "Nelson Peltz - Trian Fund Management",
    "vg": "Viking Global Investors",
    "VA": "ValueAct Capital",
    "AC": "Chuck Akre - Akre Capital Management",
    "HC": "Li Lu - Himalaya Capital Management",
    "PC": "Norbert Lou - Punch Card Management",
    "WP": "David Rolfe - Wedgewood Partners",
    "tp": "Daniel Loeb - Third Point",
    "psc": "Bill Ackman - Pershing Square Capital Management",
    "GFT": "Bill & Melinda Gates Foundation Trust",
    "SAM": "Michael Burry - Scion Asset Management",
    "ic": "Carl Icahn - Icahn Capital Management",
    "CCM": "Glenn Greenberg - Brave Warrior Advisors",
    "LPC": "Stephen Mandel - Lone Pine Capital",
    "AIM": "Alex Roepers - Atlantic Investment Management",
    "BRK": "Warren Buffett - Berkshire Hathaway",
    "VFC": "Valley Forge Capital Management",
    "KB": "Kahn Brothers Group",
    "ENG": "Glenn Welling - Engaged Capital",
    "TGM": "Chase Coleman - Tiger Global Management",
    "SP": "Dennis Hong - ShawSpring Partners",
    "LMM": "Bill Miller - Miller Value Partners",
    "GLRE": "David Einhorn - Greenlight Capital",
    "oa": "Leon Cooperman",
    "OCL": "Bryan Lawrence - Oakcliff Capital",
    "HH": "Duan Yongping - H&H International Investment",
    "AP": "AltaRock Partners",
    "CAS": "Clifford Sosin - CAS Investment Partners",
    "DA": "Pat Dorsey - Dorsey Asset Management",
    "tci": "Chris Hohn - TCI Fund Management",
    "GLC": "Josh Tarasoff - Greenlea Lane Capital",
    "FS": "Terry Smith - Fundsmith",
    "FFH": "Prem Watsa - Fairfax Financial Holdings",
    "CM": "Greg Alexander - Conifer Management",
    "oc": "Howard Marks - Oaktree Capital Management",
    "BAUPOST": "Seth Klarman - Baupost Group",
    "CAU": "Sarah Ketterer - Causeway Capital Management",
    "ca": "Francis Chou - Chou Associates",
    "TFP": "Triple Frond Partners",
    "pcm": "Polen Capital Management",
    "AKO": "AKO Capital",
    "GR": "Thomas Russo - Gardner Russo & Quinn",
    "TA": "Third Avenue Management",
    "GC": "Francois Rochon - Giverny Capital",
    "PTNT": "Samantha McLemore - Patient Capital Management",
    "PI": "Mohnish Pabrai - Pabrai Investments",
    "EC": "John Armitage - Egerton Capital",
    "JIM": "Jensen Investment Management",
    "abc": "David Abrams - Abrams Capital Management",
    "LT": "Lindsell Train",
    "DAV": "Christopher Davis - Davis Advisors",
    "FE": "First Eagle Investment Management",
    "YAM": "Yacktman Asset Management",
    "SSHFX": "Harry Burn - Sound Shore",
    "T": "Torray Funds",
    "WVALX": "Wallace Weitz - Weitz Large Cap Equity Fund",
    "MKL": "Thomas Gayner - Markel Group",
    "cc": "William Von Mueffling - Cantillon Capital Management",
    "OFALX": "Robert Olstein - Olstein Capital Management",
    "aq": "Guy Spier - Aquamarine Capital",
    "pzfvx": "Richard Pzena - Hancock Classic Value",
    "RVC": "Robert Vinall - RV Capital GmbH",
    "GA": "Greenhaven Associates"
}


# ============================================================
# VARIANTE 1 : Buy + Add + New (déjà existante)
# ============================================================

def scrape_investor_holdings(code, name):
    """Récupère les lignes Buy/Add/New pour un investisseur donné"""
    url = f"https://www.dataroma.com/m/holdings.php?m={code}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.1 Safari/537.36"
        )
    }
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f"❌ {name}: HTTP {res.status_code}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table", id="grid")
    if not table:
        print(f"❌ {name}: pas de tableau trouvé")
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        ticker = tds[1].get_text(strip=True).split(" ")[0]
        activity = tds[3].get_text(strip=True)
        pct_text = tds[2].get_text(strip=True).replace("%", "")
        try:
            pct_of_portfolio = float(pct_text)
        except ValueError:
            pct_of_portfolio = None

        if any(x in activity for x in ["Buy", "Add", "New"]):
            rows.append({
                "investor": name,
                "manager_code": code,
                "ticker": ticker,
                "activity": activity,
                "pct_of_portfolio": pct_of_portfolio,
                "source_url": url
            })

    print(f"> {name} ... {len(rows)} Buy/Add/New trouvés")
    return rows


# ============================================================
# VARIANTE 2 : Buy uniquement
# ============================================================

def scrape_buy_only(code, name):
    """Récupère uniquement les lignes 'Buy'"""
    url = f"https://www.dataroma.com/m/holdings.php?m={code}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.1 Safari/537.36"
        )
    }
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f"❌ {name}: HTTP {res.status_code}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table", id="grid")
    if not table:
        print(f"❌ {name}: pas de tableau trouvé")
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        ticker = tds[1].get_text(strip=True).split(" ")[0]
        activity = tds[3].get_text(strip=True)
        pct_text = tds[2].get_text(strip=True).replace("%", "")
        try:
            pct_of_portfolio = float(pct_text)
        except ValueError:
            pct_of_portfolio = None

        if "Buy" in activity:
            rows.append({
                "investor": name,
                "manager_code": code,
                "ticker": ticker,
                "activity": activity,
                "pct_of_portfolio": pct_of_portfolio,
                "source_url": url
            })

    print(f"> {name} ... {len(rows)} BUY trouvés")
    return rows

# ============================================================
# EXPORT MULTI-PAGES HTML
# ============================================================

def export_HTML(df_buy_only):

    now = datetime.datetime.now(UTC)
    timestamp = now.strftime("%Y-%m-%dT%H:%MZ")
    quarter = (now.month - 1) // 3 + 1

    css = """
    body { font-family: Arial, sans-serif; margin: 20px; background: #fafafa; color: #222; }
    h1, h2, h3 { color: #333; }
    table { border-collapse: collapse; width: 100%; margin-top: 20px; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    a { color: #0066cc; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .flag { font-weight: bold; color: darkred; }
    footer { margin-top: 40px; font-size: 0.9em; color: #666; }
    nav a { margin-right: 20px; font-weight: bold; }
    select { margin-top: 10px; padding: 5px; }
    """

    js_filter = """
    <script>
    function filterFlag(flag) {
        const rows = document.querySelectorAll("tbody tr");
        rows.forEach(r => {
            const val = r.cells[r.cells.length - 1].textContent.trim();
            if (flag === "ALL" || val.includes(flag)) {
                r.style.display = "";
            } else {
                r.style.display = "none";
            }
        });
    }
    </script>
    """

    df_buy_display = df_buy_only.copy()

    # ---------------------
    # PAGE BUY ONLY
    # ---------------------
    html_buy = f"""
    <html><head><meta charset="utf-8"><title>DATAROMA - BUY uniquement</title>
    <style>{css}</style>{js_filter}</head>
    <body>
    <h2>DATAROMA — Transactions BUY ONLY</h2>
    <p>Timestamp {timestamp}</p>

    <label>Filter by :</label>
    <select onchange="filterFlag(this.value)">
        <option value="ALL">All</option>
        <option value="O">Overlap (O)</option>
        <option value="P">Ponderation >15% (P)</option>
    </select>

    {df_buy_display.to_html(index=False, escape=True)}
    <footer>Source : dataroma.com</footer>
    </body></html>
    """
    with open("dataroma_BUY_ONLY.html", "w", encoding="utf-8") as f:
        f.write(html_buy)

# ============================================================
# AGRÉGATION ET FLAG
# ============================================================

def aggregate_and_flag(data, min_investors=2, pct_threshold=15):
    """Regroupe par ticker et ajoute des flags O/P"""
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data

    if df.empty:
        print("⚠️ Aucun résultat à agréger.")
        return pd.DataFrame()

    agg_dict = {
        "investor": lambda x: list(set(x)),
        "pct_of_portfolio": "mean",
    }

    grouped = df.groupby("ticker").agg(agg_dict).reset_index()
    grouped["investor_count"] = grouped["investor"].apply(len)

    grouped["flag"] = ""
    grouped.loc[grouped["investor_count"] >= min_investors, "flag"] += "O"
    grouped.loc[grouped["pct_of_portfolio"] > pct_threshold, "flag"] += "P"

    grouped = grouped.sort_values(
        by=["investor_count", "pct_of_portfolio"],
        ascending=[False, False]
    )
    return grouped

# ============================================================
# PUSH
# ============================================================

def push_to_git():
    try:

        repo_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(repo_path)

        git_path = r"C:\Program Files\Git\cmd\git.exe" if os.name == "nt" else "git"

        html_files = glob.glob("*.html")

        if not html_files:
            print("⚠️ Aucun fichier HTML trouvé à ajouter.")
            return

        # Étape 1 : add
        subprocess.run([git_path, "add"] + html_files, check=True)
        print(f"🧩 Fichiers ajoutés : {', '.join(html_files)}")

        # Étape 2 : commit
        commit_message = "📈 MAJ auto post-scraping"
        subprocess.run([git_path, "commit", "-m", commit_message], check=False)

        # Étape 3 : push
        print("\n🚀 Push automatique vers GitHub...")
        subprocess.run([git_path, "push"], check=True)

        print("✅ HTML poussé sur GitHub avec succès !")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Erreur Git (code {e.returncode}) : {e}")
    except Exception as e:
        print(f"❌ Erreur inattendue lors du push GitHub : {e}")

# ============================================================
# MAIN
# ============================================================

def main():
    collected_buy = []
    for code, name in INVESTORS.items():
        collected_buy.extend(scrape_buy_only(code, name))

    print(f"Collected total Buy-only rows: {len(collected_buy)}")
    df_buy = aggregate_and_flag(collected_buy, min_investors=2)

    export_HTML(df_buy)

# ============================================================
# LANCEMENT
# ============================================================

if __name__ == "__main__":
    main()
    # push_to_git()
