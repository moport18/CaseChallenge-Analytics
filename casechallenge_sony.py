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

#formatierung
def fmt_number(n):
    s = "{:,.0f}".format(n)
    return s.replace(",", "'")  # Tausendertrennzeichen


def get_german_gender_text(gender):
    if gender == "MALE":
        return "männlich"
    else:
        return "weiblich"


def get_german_gender_text_primary(gender):
    if gender == "MALE":
        return "männliche"
    else:
        return "weibliche"


def generate_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8')


def print_distribution(df, group_col, total_streams):
    groups = sorted(df[group_col].unique())
    for group in groups:
        streams_group = df[df[group_col] == group]["total_streams"].sum()
        pct = round(streams_group / total_streams * 100, 1)
        bars = "|" * int(pct / 3)
        
        if group_col == 'gender':
            display_text = get_german_gender_text(group)
        else:
            display_text = group
            
        print(f"{display_text:10} {pct:5.1f}% {bars}")


# main
def main():
    print_section("Case Challenge - Analytics")
    data = load_data()
    if data is None:
        print("Analyse failed.")
        return
    
    generate_csv(data, "data.csv")
    
    best_track, stats = recommend_track(data)
    generate_csv(stats, "track_scores.csv")
    
    audience_df = audience_analysis(data, best_track['track_name'], best_track['artist_name'])
    generate_csv(audience_df, "audience_analysis.csv")
    
    print_section("Analyse completed")
    print(f"Empfehlung: {best_track['track_name']} - {best_track['artist_name']}")
    audience_info = audience_df.iloc[0].to_dict()
    print(f"Kernzielgruppe: {audience_info['kernzielgruppe']} ({audience_info['anteil']:.1f}%)")


#---Aufgabe 1.1
# daten laden
def load_data():
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
        tracks = pd.merge(tracks_perf, tracks_meta[['isrc','name','artist_id','release_date']],
                            on='isrc', how='left')
        artists_tracks = pd.merge(tracks, artists[['artist_id','artist_name']],
                            on='artist_id', how='left')
        artists_tracks = artists_tracks.rename(columns={'name': 'track_name'})
        
        print("Kombinierte Datensätze:", len(artists_tracks))
        generate_csv(artists_tracks, "combined_tracks.csv")
        
        return artists_tracks
    
    except Exception as e:
        print("Fehler beim Laden der Daten:", e)
        return None


#---Aufgabe 1.2
# track empfehlung mit ScoringModell
def recommend_track(df):
    print_section("2. Track-Empfehlung")
    
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
    
    # Score = 0.5 * SaveRateNorm - 0.3 * SkipRateNorm + 0.2 * StreamShare
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
        print("\n" + f"{i+1}.", row['track_name'], "-", row['artist_name'])
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
    
    generate_csv(grouped, "track_scores.csv")
    
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
    print_distribution(track_df, 'age_group', total_streams)
    
    # Geschlechterverteilung
    print("\nGeschlechtsverteilung:")
    print_distribution(track_df, 'gender', total_streams)
    
    # Kernzielgruppe
    grouped = track_df.groupby(['age_group', 'gender'])['total_streams'].sum()
    age_gender_pct = round(grouped / total_streams * 100, 1)
    
    primary = age_gender_pct.idxmax()
    primary_share = age_gender_pct.max()
    age_group, gender = primary
    
    gender_text = get_german_gender_text_primary(gender)
    
    print("\nKernzielgruppe:")
    print(f"• {gender_text} Hörer in {age_group}, Anteil: {primary_share:.1f}%")
    
    best_audience = {
        "track": track_name,
        "artist": artist_name,
        "kernzielgruppe": f"{gender_text} {age_group}",
        "anteil": primary_share,
    }
    
    return pd.DataFrame([best_audience])


if __name__ == "__main__":
    main()