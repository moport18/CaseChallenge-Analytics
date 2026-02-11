import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def print_section(title):
    print("\n" + "="*40)
    print(" " + title)
    print("="*40)

def fmt_number(n):
    s = "{:,.0f}".format(n)
    return s.replace(",", "'")  # Tausendertrennzeichen

#---Aufgabe 1.1
# daten laden
def load_combine_data():
    print_section("1. Daten laden & kombinieren")
    
    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Supabase-Verbindung succsessful")
        
        tracks_perf_data = client.table("sme_track_data").select("*").execute().data
        tracks_meta_data = client.table("sme_track").select("*").execute().data
        artists_data = client.table("sme_artist").select("*").execute().data
        
        tracks_perf = pd.DataFrame(tracks_perf_data)
        tracks_meta = pd.DataFrame(tracks_meta_data)
        artists = pd.DataFrame(artists_data)
        
        print("Tracks Performance:", len(tracks_perf))
        print("Tracks Meta:", len(tracks_meta))
        print("Artists:", len(artists))
        
        # Tabellen kombinieren
        combined = pd.merge(tracks_perf, tracks_meta[['isrc','name','artist_id','release_date']],
                            on='isrc', how='left')
        combined = pd.merge(combined, artists[['artist_id','artist_name']],
                            on='artist_id', how='left')
        combined = combined.rename(columns={'name': 'track_name'})
        
        print("Kombinierte Datensätze:", len(combined))
        combined.to_csv("combined_tracks.csv", index=False, encoding='utf-8')
        
        return combined
    
    except Exception as e:
        print("Fehler beim Laden der Daten:", e)
        return None


#---Aufgabe 1.2
# track empfehlung mit ScoringModell
def recommend_track(df):
    print_section("2. Track-Empfehlung")
    print("Scoring-Modell: 50% Save-Rate | 30% Skip-Rate | 20% Stream-Share")
    
    # Aggregieren auf Track-Ebene
    grouped = df.groupby(['track_name', 'artist_name']).agg({
        'total_streams': 'sum',
        'total_saves': 'sum',
        'skip_rate': 'mean'
    }).reset_index()
    
    
    grouped['save_rate'] = grouped['total_saves'] / grouped['total_streams']
    
    grouped['skip_rate'] = grouped['skip_rate'] / 100
    
    # Normierungskonstanten
    SAVE_RATE_AVG = grouped['save_rate'].mean()  # Durchschnittliche Save-Rate im Set
    SKIP_RATE_AVG = grouped['skip_rate'].mean()  # Durchschnittliche Skip-Rate im Set
    TOTAL_STREAMS_ALL = grouped['total_streams'].sum()  # Gesamtstreams aller Tracks
    
    # Scoring Formel
    grouped['save_rate_norm'] = grouped['save_rate'] / SAVE_RATE_AVG
    grouped['skip_rate_norm'] = grouped['skip_rate'] / SKIP_RATE_AVG
    grouped['stream_share'] = grouped['total_streams'] / TOTAL_STREAMS_ALL
    
    # Hauptformel: Score = 0.5 * SaveRateNorm - 0.3 * SkipRateNorm + 0.2 * StreamShare
    grouped['total_score'] = (0.5 * grouped['save_rate_norm'] - 
                              0.3 * grouped['skip_rate_norm'] + 
                              0.2 * grouped['stream_share'])
    
    # Sortieren
    grouped = grouped.sort_values('total_score', ascending=False).reset_index(drop=True)
    
    print("-"*40)
    print("Tracks im Vergleich: ")
    print("-"*40)
    
    for i in range(len(grouped)):
        row = grouped.iloc[i]
        if i == 0:
            rank = "1"
        elif i == 1:
            rank = "2"
        elif i == 2:
            rank = "3"
        else:
            rank = f"{i+1}."
        
        print("\n" + rank, row['track_name'], "-", row['artist_name'])
        print("   Streams:", fmt_number(row['total_streams']))
        print(f"   Stream-Share: {row['stream_share']:.4f} ({row['stream_share']*100:.2f}%)")
        print(f"   Save-Rate: {row['save_rate']:.3f} ({row['save_rate']*100:.1f}%)")
        print(f"   Save-Rate-Norm: {row['save_rate_norm']:.4f}")
        print(f"   Skip-Rate: {row['skip_rate']:.4f} ({row['skip_rate']*1000:.0f}‰)")
        print(f"   Skip-Rate-Norm: {row['skip_rate_norm']:.4f}")
        print(f"   Score: {row['total_score']:.4f}")
    
    best = grouped.iloc[0]
  
    print(" \nFokus für nächsten Monat:")
    print(f"**{best['track_name']}** von **{best['artist_name']}**")
    print(f"Score: {best['total_score']:.4f}")
    print("\nBegründung:")
    print(f"• Engagement (Save-Rate): {best['save_rate']:.3f} vs. Ø {SAVE_RATE_AVG:.3f}")
    print(f"• Qualität (Skip-Rate): {best['skip_rate']:.4f} vs. Ø {SKIP_RATE_AVG:.4f}")
    print(f"• Volumen (Stream-Share): {best['stream_share']*100:.2f}%")
    print("\nGewichtung: 50% Save-Rate , 30% Skip-Rate (negativ) , 20% Stream-Share")
    
    grouped.to_csv("track_scores.csv", index=False, encoding='utf-8')
    
    return best, grouped

#---Aufgabe1.3
# zielgruppen analyse
def audience_analysis(df, track_name, artist_name):
    print_section("3. Zielgruppenanalyse")
    
    track_df = df[(df['track_name']==track_name) & (df['artist_name']==artist_name)]
    total_streams = track_df['total_streams'].sum()
    print(f"Track: {track_name} von {artist_name}, Gesamt-Streams: {fmt_number(total_streams)}")
    
    # Altersverteilung
    print("\nAltersverteilung:")
  
    age_groups = sorted(track_df['age_group'].unique())
    age_pct_dict = {}
    for age in age_groups:
        streams_age = track_df[track_df['age_group']==age]['total_streams'].sum()
        pct = round(streams_age / total_streams * 100, 1)
        age_pct_dict[age] = pct
        bars = "█" * int(pct / 3)
        print(f"{age:10} {pct:5.1f}% {bars}")
    
    # Geschlechterverteilung
    print("\nGeschlechtsverteilung:")

    genders = track_df['gender'].unique()
    gender_pct_dict = {}
    for g in genders:
        streams_gender = track_df[track_df['gender']==g]['total_streams'].sum()
        pct = round(streams_gender / total_streams * 100, 1)
        gender_pct_dict[g] = pct
        gender_text = "männlich" if g=="MALE" else "weiblich"
        bars = "█" * int(pct / 3)
        print(f"{gender_text:10} {pct:5.1f}% {bars}")
    
    # Kernzielgruppe
    age_gender_pct = {}
    for age in age_groups:
        for g in genders:
            streams = track_df[(track_df['age_group']==age) & (track_df['gender']==g)]['total_streams'].sum()
            pct = round(streams / total_streams * 100, 1)
            age_gender_pct[(age,g)] = pct
    primary = max(age_gender_pct, key=age_gender_pct.get)
    primary_share = age_gender_pct[primary]
    age_group, gender = primary
    gender_text = "männliche" if gender=="MALE" else "weibliche"
    
    print("\nKernzielgruppe:")
    print(f"• {gender_text} Hörer in {age_group}, Anteil: {primary_share:.1f}%")
    
    result = pd.DataFrame([{
        'track': track_name,
        'artist': artist_name,
        'kernzielgruppe': f"{gender_text} {age_group}",
        'anteil': primary_share
    }])
    result.to_csv("audience_analysis.csv", index=False, encoding='utf-8')
    
    return {
        'track': track_name,
        'artist': artist_name,
        'kernzielgruppe': f"{gender_text} {age_group}",
        'anteil': primary_share
    }

# main
def main():
    print_section("Case Challenge - Analytics")
    data = load_combine_data()
    if data is None:
        print("Analyse failed.")
        return
    
    best_track, stats = recommend_track(data)
    audience_info = audience_analysis(data, best_track['track_name'], best_track['artist_name'])
    
    print_section("Analyse completed")
    print(f"Empfehlung: {best_track['track_name']} - {best_track['artist_name']}")
    print(f"Kernzielgruppe: {audience_info['kernzielgruppe']} ({audience_info['anteil']:.1f}%)")

if __name__ == "__main__":
    main()